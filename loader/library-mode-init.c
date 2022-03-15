/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "api-versions.h"
#include "config-parser.h"
#include "loader.h"
#include <nanos6/bootstrap.h>
#include <nanos6/library-mode.h>


__attribute__ ((used)) char const * nanos6_library_mode_init_cluster(int argc, char **argv)
{
	if (nanos6_check_api_versions(&__user_code_expected_nanos6_api_versions) != 1) {
		snprintf(_nanos6_error_text, ERROR_TEXT_SIZE, "This executable was compiled for a different version of Nanos6.");
		_nanos6_exit_with_error = 1;

		return _nanos6_error_text;
	}

	nanos6_preinit(argc, argv);
	if (_nanos6_exit_with_error) {
		return _nanos6_error_text;
	}

	nanos6_init();
	if (_nanos6_exit_with_error) {
		return _nanos6_error_text;
	}

	char *instrument = _config.instrument;
	if (instrument != NULL) {
		if (!strcmp(instrument, "graph")) {
			fprintf(stderr, "Warning: Graph variants may yield incorrect results when using nanos6 library mode\n");
		}
	}

	return NULL;
}

__attribute__ ((used)) char const * nanos6_library_mode_init(void)
{
	// This makes nanos6 library mode incompatible with Cluster mode because argc and argv are
	// enforced in cluster mode.  We can't add cluster checks here because it is too early, but
	// maybe latter we find something to assert we don't use this in cluster mode.  Changing this
	// may imply that the API in api/nanos6/library-mode.h will change too; which may be a backward
	// compatibility break with some applications.  Alternatively that's why we need
	// nanos6_library_mode_init_cluster and converted this one in a wrapper..
	return nanos6_library_mode_init_cluster(0, NULL);
}

