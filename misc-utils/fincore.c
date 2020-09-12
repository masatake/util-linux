/*
 * fincore - count pages of file contents in core
 *
 * Copyright (C) 2017 Red Hat, Inc. All rights reserved.
 * Written by Masatake YAMATO <yamato@redhat.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it would be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <string.h>

#ifdef HAVE_NUMA
#include <numa.h>
#include <numaif.h>
#define NUMA_EXT
#else
#define NUMA_EXT __attribute__((__unused__))
#endif	/* HAVE_NUMA */

#include "c.h"
#include "nls.h"
#include "closestream.h"
#include "xalloc.h"
#include "strutils.h"

#ifdef HAVE_NUMA
#include "strv.h"
#endif	/* HAVE_NUMA */

#include "libsmartcols.h"

/* For large files, mmap is called in iterative way.
   Window is the unit of vma prepared in each mmap
   calling.

   Window size depends on page size.
   e.g. 128MB on x86_64. ( = N_PAGES_IN_WINDOW * 4096 ). */
#define N_PAGES_IN_WINDOW ((size_t)(32 * 1024))


struct colinfo {
	const char *name;
	double whint;
	int flags;
	const char *help;
};

enum {
	COL_PAGES,
	COL_SIZE,
	COL_FILE,
#ifdef HAVE_NUMA
	COL_NODEDST,
#endif	/* HAVE_NUMA */
	COL_RES
};

static struct colinfo infos[] = {
	[COL_PAGES]  = { "PAGES",    1, SCOLS_FL_RIGHT, N_("file data resident in memory in pages")},
	[COL_RES]    = { "RES",      5, SCOLS_FL_RIGHT, N_("file data resident in memory in bytes")},
	[COL_SIZE]   = { "SIZE",     5, SCOLS_FL_RIGHT, N_("size of the file")},
	[COL_FILE]   = { "FILE",     4, 0, N_("file name")},
#ifdef HAVE_NUMA
	[COL_NODEDST]= { "NODEDST",  7, 0, N_("pages distribution in numa nodes")},
#endif	/* HAVE_NUMA */
};

static int columns[ARRAY_SIZE(infos) * 2] = {-1};
static size_t ncolumns;

struct fincore_control {
	const size_t pagesize;

	struct libscols_table *tb;		/* output */

	unsigned int bytes : 1,
		     noheadings : 1,
		     raw : 1,
		     json : 1;

	int drop;
};


static int column_name_to_id(const char *name, size_t namesz)
{
	size_t i;

	for (i = 0; i < ARRAY_SIZE(infos); i++) {
		const char *cn = infos[i].name;

		if (!strncasecmp(name, cn, namesz) && !*(cn + namesz))
			return i;
	}
	warnx(_("unknown column: %s"), name);
	return -1;
}

static int get_column_id(int num)
{
	assert(num >= 0);
	assert((size_t) num < ncolumns);
	assert(columns[num] < (int) ARRAY_SIZE(infos));
	return columns[num];
}

static const struct colinfo *get_column_info(int num)
{
	return &infos[ get_column_id(num) ];
}

static int add_output_data(struct fincore_control *ctl,
			   const char *name,
			   off_t file_size,
			   off_t count_incore,
			   int *nodes_counter NUMA_EXT)
{
	size_t i;
	char *tmp;
	struct libscols_line *ln;

	assert(ctl);
	assert(ctl->tb);

	ln = scols_table_new_line(ctl->tb, NULL);
	if (!ln)
		err(EXIT_FAILURE, _("failed to allocate output line"));

	for (i = 0; i < ncolumns; i++) {
		int rc = 0;

		switch(get_column_id(i)) {
		case COL_FILE:
			rc = scols_line_set_data(ln, i, name);
			break;
		case COL_PAGES:
			xasprintf(&tmp, "%jd",  (intmax_t) count_incore);
			rc = scols_line_refer_data(ln, i, tmp);
			break;
		case COL_RES:
		{
			uintmax_t res = (uintmax_t) count_incore * ctl->pagesize;

			if (ctl->bytes)
				xasprintf(&tmp, "%ju", res);
			else
				tmp = size_to_human_string(SIZE_SUFFIX_1LETTER, res);
			rc = scols_line_refer_data(ln, i, tmp);
			break;
		}
		case COL_SIZE:
			if (ctl->bytes)
				xasprintf(&tmp, "%jd", (intmax_t) file_size);
			else
				tmp = size_to_human_string(SIZE_SUFFIX_1LETTER, file_size);
			rc = scols_line_refer_data(ln, i, tmp);
			break;
#ifdef HAVE_NUMA
		case COL_NODEDST:
			if (!nodes_counter)
				break;
			char ** v = NULL;
			for (int i = 0; i < NUMA_NUM_NODES; i++) {
				if (nodes_counter[i] != 0)
					strv_extendf (&v, "[%d]=%d ", i, nodes_counter[i]);
			}
			if (v) {
				scols_line_refer_data(ln, i, strv_join (v, " "));
				strv_free (v);
			}
			break;
#endif	/* HAVE_NUMA */
		default:
			return -EINVAL;
		}

		if (rc)
			err(EXIT_FAILURE, _("failed to add output data"));
	}

	return 0;
}

static int do_mincore(struct fincore_control *ctl,
		      void *window, const size_t len,
		      const char *name,
		      off_t *count_incore,
		      int *nodes_counter NUMA_EXT)
{
	static unsigned char vec[N_PAGES_IN_WINDOW];
	int n = (len / ctl->pagesize) + ((len % ctl->pagesize)? 1: 0);

	if (mincore (window, len, vec) < 0) {
		warn(_("failed to do mincore: %s"), name);
		return -errno;
	}

	off_t original_count_incore  NUMA_EXT = *count_incore;
	static void* incore_pages[N_PAGES_IN_WINDOW] NUMA_EXT;
	while (n > 0)
	{
		if (vec[--n] & 0x1)
		{
#ifdef HAVE_NUMA
			if (nodes_counter) {
				off_t d = *count_incore - original_count_incore;
				incore_pages [d] = (void *)(((char *)window) + (ctl->pagesize * n));
				volatile char c __attribute__((__unused__)) = *(char*)(incore_pages [d]);
			}
#endif	/* HAVE_NUMA */
			vec[n] = 0;
			(*count_incore)++;
		}
	}

#ifdef HAVE_NUMA
	if (!nodes_counter)
		return 0;

	off_t d = *count_incore - original_count_incore;
	static int status[N_PAGES_IN_WINDOW];
	if (move_pages (0, (int)d, incore_pages, NULL, status, 0/* MPOL_MF_MOVE_ALL*/) < 0) {
		warn(_("failed to do move_pages: %s"), name);
		return -errno;
	}
	for (int i = 0; i < (int)d; i++) {
		if (status [i] < 0)
			continue;
		nodes_counter [status [i]]++;
	}
#endif	/* HAVE_NUMA */

	return 0;
}

static int fincore_fd (struct fincore_control *ctl,
		       int fd,
		       const char *name,
		       off_t file_size,
		       off_t *count_incore,
		       int   *nodes_counter NUMA_EXT)
{
	size_t window_size = N_PAGES_IN_WINDOW * ctl->pagesize;
	off_t file_offset, len;
	int rc = 0;

	for (file_offset = 0; file_offset < file_size; file_offset += len) {
		void  *window = NULL;

		len = file_size - file_offset;
		if (len >= (off_t) window_size)
			len = window_size;

		int prot = nodes_counter? PROT_READ: 0;
		window = mmap(window, len, prot, MAP_SHARED, fd, file_offset);
		if (window == MAP_FAILED) {
			rc = -EINVAL;
			warn(_("failed to do mmap: %s"), name);
			break;
		}

		rc = do_mincore(ctl, window, len, name, count_incore, nodes_counter);
		if (rc)
			break;

		munmap (window, len);
	}

	return rc;
}

/*
 * Returns: <0 on error, 0 success, 1 ignore.
 */
static int fincore_name(struct fincore_control *ctl,
			const char *name,
			struct stat *sb,
			off_t *count_incore,
			int *nodes_counter NUMA_EXT)
{
	int fd;
	int rc = 0;

	if ((fd = open (name, O_RDONLY)) < 0) {
		warn(_("failed to open: %s"), name);
		return -errno;
	}

	if (fstat (fd, sb) < 0) {
		warn(_("failed to do fstat: %s"), name);
		close (fd);
		return -errno;
	}

	if (S_ISDIR(sb->st_mode))
		rc = 1;			/* ignore */

	else if (sb->st_size) {
		if (ctl->drop) {
			rc = posix_fadvise(fd, 0, sb->st_size, POSIX_FADV_DONTNEED);
			if (rc < 0)
				warn(_("failed to do posix_fadvise: %s"), name);
		}
		rc = fincore_fd(ctl, fd, name, sb->st_size, count_incore, nodes_counter);
	}

	close (fd);
	return rc;
}

static void __attribute__((__noreturn__)) usage(void)
{
	FILE *out = stdout;
	size_t i;

	fputs(USAGE_HEADER, out);
	fprintf(out, _(" %s [options] file...\n"), program_invocation_short_name);

	fputs(USAGE_OPTIONS, out);
	fputs(_(" -J, --json            use JSON output format\n"), out);
	fputs(_(" -b, --bytes           print sizes in bytes rather than in human readable format\n"), out);
	fputs(_(" -n, --noheadings      don't print headings\n"), out);
	fputs(_(" -o, --output <list>   output columns\n"), out);
	fputs(_(" -r, --raw             use raw output format\n"), out);
	fputs(_(" -d, --drop            try to drop pages before counting\n"), out);

	fputs(USAGE_SEPARATOR, out);
	printf(USAGE_HELP_OPTIONS(23));

	fprintf(out, USAGE_COLUMNS);

	for (i = 0; i < ARRAY_SIZE(infos); i++)
		fprintf(out, " %11s  %s\n", infos[i].name, _(infos[i].help));

	printf(USAGE_MAN_TAIL("fincore(1)"));

	exit(EXIT_SUCCESS);
}

int main(int argc, char ** argv)
{
	int c;
	size_t i;
	int rc = EXIT_SUCCESS;
	char *outarg = NULL;
	int collect_nodedst = 0;

	struct fincore_control ctl = {
		.drop     = 0,
		.pagesize = getpagesize()
	};

	static const struct option longopts[] = {
		{ "bytes",      no_argument, NULL, 'b' },
		{ "drop",       no_argument, NULL, 'd' },
		{ "noheadings", no_argument, NULL, 'n' },
		{ "output",     required_argument, NULL, 'o' },
		{ "version",    no_argument, NULL, 'V' },
		{ "help",	no_argument, NULL, 'h' },
		{ "json",       no_argument, NULL, 'J' },
		{ "raw",        no_argument, NULL, 'r' },
		{ NULL, 0, NULL, 0 },
	};

	setlocale(LC_ALL, "");
	bindtextdomain(PACKAGE, LOCALEDIR);
	textdomain(PACKAGE);
	close_stdout_atexit();

	while ((c = getopt_long (argc, argv, "bdno:JrVh", longopts, NULL)) != -1) {
		switch (c) {
		case 'b':
			ctl.bytes = 1;
			break;
		case 'd':
			ctl.drop = 1;
			break;
		case 'n':
			ctl.noheadings = 1;
			break;
		case 'o':
			outarg = optarg;
			break;
		case 'J':
			ctl.json = 1;
			break;
		case 'r':
			ctl.raw = 1;
			break;
		case 'V':
			print_version(EXIT_SUCCESS);
		case 'h':
			usage();
		default:
			errtryhelp(EXIT_FAILURE);
		}
	}

	if (optind == argc) {
		warnx(_("no file specified"));
		errtryhelp(EXIT_FAILURE);
	}

	if (!ncolumns) {
		columns[ncolumns++] = COL_RES;
		columns[ncolumns++] = COL_PAGES;
		columns[ncolumns++] = COL_SIZE;
		columns[ncolumns++] = COL_FILE;
	}

	if (outarg && string_add_to_idarray(outarg, columns, ARRAY_SIZE(columns),
					 &ncolumns, column_name_to_id) < 0)
		return EXIT_FAILURE;

	scols_init_debug(0);
	ctl.tb = scols_new_table();
	if (!ctl.tb)
		err(EXIT_FAILURE, _("failed to allocate output table"));

	scols_table_enable_noheadings(ctl.tb, ctl.noheadings);
	scols_table_enable_raw(ctl.tb, ctl.raw);
	scols_table_enable_json(ctl.tb, ctl.json);
	if (ctl.json)
		scols_table_set_name(ctl.tb, "fincore");

	for (i = 0; i < ncolumns; i++) {
		const struct colinfo *col = get_column_info(i);
		struct libscols_column *cl;
		int id = get_column_id(i);

		cl = scols_table_new_column(ctl.tb, col->name, col->whint, col->flags);
		if (!cl)
			err(EXIT_FAILURE, _("failed to allocate output column"));

#ifdef HAVE_NUMA
		if (id == COL_NODEDST)
			collect_nodedst = 1;
#endif	/* HAVE_NUMA */

		if (ctl.json) {
			switch (id) {
			case COL_FILE:
				scols_column_set_json_type(cl, SCOLS_JSON_STRING);
				break;
			case COL_SIZE:
			case COL_RES:
				if (!ctl.bytes)
					break;
				/* fallthrough */
			default:
				scols_column_set_json_type(cl, SCOLS_JSON_NUMBER);
				break;
			}
		}
	}

	for(; optind < argc; optind++) {
		char *name = argv[optind];
		struct stat sb;
		off_t count_incore = 0;
#ifdef HAVE_NUMA
		int  nodes_counter [NUMA_NUM_NODES] = {0, };
#else
		int *nodes_counter = NULL;
#endif

		switch (fincore_name(&ctl, name, &sb, &count_incore,
				     collect_nodedst? nodes_counter: NULL)) {
		case 0:
			add_output_data(&ctl, name, sb.st_size, count_incore,
					collect_nodedst? nodes_counter: NULL);
			break;
		case 1:
			break; /* ignore */
		default:
			rc = EXIT_FAILURE;
			break;
		}
	}

	scols_print_table(ctl.tb);
	scols_unref_table(ctl.tb);

	return rc;
}
