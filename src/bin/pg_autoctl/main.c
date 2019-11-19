/*
 * src/bin/pg_autoctl/main.c
 *    Main entry point for the pg_autoctl command-line tool
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */

#include <getopt.h>
#include <unistd.h>

#include "postgres_fe.h"

#include "cli_root.h"
#include "keeper.h"
#include "keeper_config.h"

char pg_autoctl_argv0[MAXPGPATH];
char pg_autoctl_program[MAXPGPATH];

/*
 * Main entry point for the binary.
 */
int
main(int argc, char **argv)
{
	CommandLine command = root;

	/*
	 * Stash away the ARGV[0] used to run this program, we might need it to
	 * fill in our systemd service unit configuration file later. Also compute
	 * the realpath of the program invoked, which we need at several places.
	 */
	strlcpy(pg_autoctl_argv0, argv[0], MAXPGPATH);

	if (!get_program_absolute_path(pg_autoctl_program, MAXPGPATH))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * When PG_AUTOCTL_DEBUG is set in the environement, provide the user
	 * commands available to debug a pg_autoctl instance.
	 */
	if (getenv(PG_AUTOCTL_DEBUG) != NULL)
	{
		command = root_with_debug;
	}

	/*
	 * We need to follow POSIX specifications for argument parsing, in
	 * particular we want getopt() to stop as soon as it reaches a non option
	 * in the command line.
	 *
	 * GNU and modern getopt() implementation will reorder the command
	 * arguments, making a mess of our nice subcommands facility.
	 *
	 * Note that we call unsetenv("POSIXLY_CORRECT"); before parsing options
	 * for commands that are the final sub-command of their chain and when we
	 * might mix options and arguments.
	 */
	setenv("POSIXLY_CORRECT", "1", 1);

	/* we're verbose by default */
	log_set_level(LOG_INFO);

	/*
	 * Log messages go to stderr. We use colours when stderr is being shown
	 * directly to the user to make it easier to spot warnings and errors.
	 */
	log_use_colors(isatty(fileno(stderr)));

	(void) commandline_run(&command, argc, argv);

	return 0;
}
