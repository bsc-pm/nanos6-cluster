/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2017 Barcelona Supercomputing Center (BSC)
*/

#include "resolve.h"


#pragma GCC visibility push(default)

void nanos6_preinit(int argc, char **argv)
{
	typedef void nanos6_preinit_t(int, char **);

	static nanos6_preinit_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_preinit_t *) _nanos6_resolve_symbol("nanos6_preinit", "essential", NULL);
	}

	(*symbol)(argc, argv);
}

int nanos6_can_run_main(void)
{
	typedef int nanos6_can_run_main_t(void);

	static nanos6_can_run_main_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_can_run_main_t *) _nanos6_resolve_symbol("nanos6_can_run_main", "essential", NULL);
	}

	return (*symbol)();
}

void nanos6_register_node(void (*callback_function)(void *), void *callback_args)
{
	typedef void nanos6_register_node_t(void (*)(void *), void *);

	static nanos6_register_node_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_register_node_t *) _nanos6_resolve_symbol("nanos6_register_node", "essential", NULL);
	}

	(*symbol)(callback_function, callback_args);
}

void nanos6_init(void)
{
	typedef void nanos6_init_t(void);

	static nanos6_init_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_init_t *) _nanos6_resolve_symbol("nanos6_init", "essential", NULL);
	}

	(*symbol)();
}


void nanos6_shutdown(void)
{
	typedef void nanos6_shutdown_t(void);

	static nanos6_shutdown_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_shutdown_t *) _nanos6_resolve_symbol("nanos6_shutdown", "essential", NULL);
	}

	(*symbol)();
}


#pragma GCC visibility pop
