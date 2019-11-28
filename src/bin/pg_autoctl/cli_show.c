/*
 * src/bin/pg_autoctl/cli_show.c
 *     Implementation of a CLI to show events, states, and URI from the
 *     pg_auto_failover monitor.
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */
#include <inttypes.h>
#include <getopt.h>
#include <unistd.h>

#include "postgres_fe.h"

#include "cli_common.h"
#include "commandline.h"
#include "defaults.h"
#include "ipaddr.h"
#include "keeper_config.h"
#include "keeper.h"
#include "monitor_config.h"
#include "monitor_pg_init.h"
#include "monitor.h"
#include "pgctl.h"
#include "pghba.h"
#include "pgsetup.h"
#include "pgsql.h"
#include "state.h"

static int eventCount = 10;
static bool outputJSON = false;

static int cli_show_state_getopts(int argc, char **argv);
static void cli_show_state(int argc, char **argv);
static void cli_show_events(int argc, char **argv);

static int cli_show_files_getopts(int argc, char **argv);
static void cli_show_files(int argc, char **argv);
static bool fprint_file_contents(const char *filename);

static int cli_show_uri_getopts(int argc, char **argv);
static void cli_show_uri(int argc, char **argv);
static void cli_show_monitor_uri(int argc, char **argv);
static void cli_show_formation_uri(int argc, char **argv);

CommandLine show_uri_command =
	make_command("uri",
				 "Show the postgres uri to use to connect to pg_auto_failover nodes",
				 " [ --pgdata --formation ] ",
				 "  --pgdata      path to data directory\n"	\
				 "  --formation   show the coordinator uri of given formation\n",
				 cli_show_uri_getopts,
				 cli_show_uri);

CommandLine show_events_command =
	make_command("events",
				 "Prints monitor's state of nodes in a given formation and group",
				 " [ --pgdata --formation --group --count ] ",
				 "  --pgdata      path to data directory	 \n"		\
				 "  --formation   formation to query, defaults to 'default' \n" \
				 "  --group       group to query formation, defaults to all \n" \
				 "  --count       how many events to fetch, defaults to 10 \n",
				 cli_show_state_getopts,
				 cli_show_events);

CommandLine show_state_command =
	make_command("state",
				 "Prints monitor's state of nodes in a given formation and group",
				 " [ --pgdata --formation --group ] ",
				 "  --pgdata      path to data directory	 \n"		\
				 "  --formation   formation to query, defaults to 'default' \n" \
				 "  --group       group to query formation, defaults to all \n" \
				 "  --json        output data in the JSON format\n",
				 cli_show_state_getopts,
				 cli_show_state);


CommandLine show_files_command =
	make_command("files",
				 "List pg_autoctl internal files (config, state, pid)",
				 " [ --pgdata ] [ --config | --state | --init | --pid ] ",
				 "  --pgdata      path to data directory \n",
				 cli_show_files_getopts,
				 cli_show_files);

typedef struct ShowFilesOptions
{
	bool showFileContent;
	bool showConfig;
	bool showState;
	bool showInit;
	bool showPid;
} ShowFilesOptions;

static ShowFilesOptions showFilesOptions;

/*
 * keeper_cli_monitor_state_getopts parses the command line options for the
 * command `pg_autoctl show state`.
 */
static int
cli_show_state_getopts(int argc, char **argv)
{
	KeeperConfig options = { 0 };
	int c, option_index = 0, errors = 0;
	int verboseCount = 0;

	static struct option long_options[] = {
		{ "pgdata", required_argument, NULL, 'D' },
		{ "formation", required_argument, NULL, 'f' },
		{ "group", required_argument, NULL, 'g' },
		{ "count", required_argument, NULL, 'n' },
		{ "json", no_argument, NULL, 'J' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	/* set default values for our options, when we have some */
	options.groupId = -1;
	options.network_partition_timeout = -1;
	options.prepare_promotion_catchup = -1;
	options.prepare_promotion_walreceiver = -1;
	options.postgresql_restart_failure_timeout = -1;
	options.postgresql_restart_failure_max_retries = -1;

	strlcpy(options.formation, "default", NAMEDATALEN);

	optind = 0;

	while ((c = getopt_long(argc, argv, "D:f:g:n:Vvqh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'D':
			{
				strlcpy(options.pgSetup.pgdata, optarg, MAXPGPATH);
				log_trace("--pgdata %s", options.pgSetup.pgdata);
				break;
			}

			case 'f':
			{
				strlcpy(options.formation, optarg, NAMEDATALEN);
				log_trace("--formation %s", options.formation);
				break;
			}

			case 'g':
			{
				int scanResult = sscanf(optarg, "%d", &options.groupId);
				if (scanResult == 0)
				{
					log_fatal("--group argument is not a valid group ID: \"%s\"",
							  optarg);
					exit(EXIT_CODE_BAD_ARGS);
				}
				log_trace("--group %d", options.groupId);
				break;
			}

			case 'n':
			{
				int scanResult = sscanf(optarg, "%d", &eventCount);
				if (scanResult == 0)
				{
					log_fatal("--count argument is not a valid count: \"%s\"",
							  optarg);
					exit(EXIT_CODE_BAD_ARGS);
				}
				log_trace("--count %d", eventCount);
				break;
			}

			case 'V':
			{
				/* keeper_cli_print_version prints version and exits. */
				keeper_cli_print_version(argc, argv);
				break;
			}

			case 'v':
			{
				++verboseCount;
				switch (verboseCount)
				{
					case 1:
						log_set_level(LOG_INFO);
						break;

					case 2:
						log_set_level(LOG_DEBUG);
						break;

					default:
						log_set_level(LOG_TRACE);
						break;
				}
				break;
			}

			case 'q':
			{
				log_set_level(LOG_ERROR);
				break;
			}

			case 'h':
			{
				commandline_help(stderr);
				exit(EXIT_CODE_QUIT);
				break;
			}

			case 'J':
			{
				outputJSON = true;
				log_trace("--json");
				break;
			}

			default:
			{
				/* getopt_long already wrote an error message */
				errors++;
			}
		}
	}

	if (errors > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (IS_EMPTY_STRING_BUFFER(options.pgSetup.pgdata))
	{
		char *pgdata = getenv("PGDATA");

		if (pgdata == NULL)
		{
			log_fatal("Failed to get PGDATA either from the environment "
					  "or from --pgdata");
			exit(EXIT_CODE_BAD_ARGS);
		}

		strlcpy(options.pgSetup.pgdata, pgdata, MAXPGPATH);
	}

	/*
	 * pg_setup_init wants a single pg_ctl, and we don't use it here: pretend
	 * we had a --pgctl option and processed it.
	 */
	set_first_pgctl(&(options.pgSetup));

	keeperOptions = options;

	return optind;
}


/*
 * keeper_cli_monitor_print_events prints the list of the most recent events
 * known to the monitor.
 */
static void
cli_show_events(int argc, char **argv)
{
	KeeperConfig config = keeperOptions;
	Monitor monitor = { 0 };

	if (!monitor_init_from_pgsetup(&monitor, &config.pgSetup))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!monitor_print_last_events(&monitor,
								   config.formation,
								   config.groupId,
								   eventCount))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_MONITOR);
	}
}


/*
 * keeper_cli_monitor_print_state prints the current state of given formation
 * and port from the monitor's point of view.
 */
static void
cli_show_state(int argc, char **argv)
{
	KeeperConfig config = keeperOptions;
	Monitor monitor = { 0 };

	if (!monitor_init_from_pgsetup(&monitor, &config.pgSetup))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (outputJSON)
	{
		char json[BUFSIZE];

		if (!monitor_get_state_as_json(&monitor,
									   config.formation, config.groupId,
									   json, BUFSIZE))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_MONITOR);
		}
		fprintf(stdout, "%s\n", json);
	}
	else
	{
		if (!monitor_print_state(&monitor, config.formation, config.groupId))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_MONITOR);
		}
	}
}


/*
 * keeper_show_uri_getopts parses the command line options for the
 * command `pg_autoctl show uri`.
 */
static int
cli_show_uri_getopts(int argc, char **argv)
{
	KeeperConfig options = { 0 };
	int c, option_index = 0;
	int verboseCount = 0;

	static struct option long_options[] = {
		{ "pgdata", required_argument, NULL, 'D' },
		{ "formation", required_argument, NULL, 'f' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	/* set default values for our options, when we have some */
	options.groupId = -1;
	options.network_partition_timeout = -1;
	options.prepare_promotion_catchup = -1;
	options.prepare_promotion_walreceiver = -1;
	options.postgresql_restart_failure_timeout = -1;
	options.postgresql_restart_failure_max_retries = -1;

	optind = 0;

	while ((c = getopt_long(argc, argv, "D:f:Vvqh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'D':
			{
				strlcpy(options.pgSetup.pgdata, optarg, MAXPGPATH);
				log_trace("--pgdata %s", options.pgSetup.pgdata);
				break;
			}

			case 'f':
			{
				strlcpy(options.formation, optarg, NAMEDATALEN);
				log_trace("--formation %s", options.formation);
				break;
			}

			case 'V':
			{
				/* keeper_cli_print_version prints version and exits. */
				keeper_cli_print_version(argc, argv);
				break;
			}

			case 'v':
			{
				++verboseCount;
				switch (verboseCount)
				{
					case 1:
						log_set_level(LOG_INFO);
						break;

					case 2:
						log_set_level(LOG_DEBUG);
						break;

					default:
						log_set_level(LOG_TRACE);
						break;
				}
				break;
			}

			case 'q':
			{
				log_set_level(LOG_ERROR);
				break;
			}

			case 'h':
			{
				commandline_help(stderr);
				exit(EXIT_CODE_QUIT);
				break;
			}

			default:
			{
				log_error("Failed to parse command line, see above for details.");
				commandline_help(stderr);
				exit(EXIT_CODE_BAD_ARGS);
				break;
			}
		}
	}

	if (IS_EMPTY_STRING_BUFFER(options.pgSetup.pgdata))
	{
		char *pgdata = getenv("PGDATA");

		if (pgdata == NULL)
		{
			log_fatal("Failed to get PGDATA either from the environment "
					  "or from --pgdata");
			exit(EXIT_CODE_BAD_ARGS);
		}

		strlcpy(options.pgSetup.pgdata, pgdata, MAXPGPATH);
	}

	keeperOptions = options;

	return optind;
}


/*
 * cli_show_uri prints the URI to connect to with psql.
 */
static void
cli_show_uri(int argc, char **argv)
{
	KeeperConfig config = keeperOptions;
	if (!IS_EMPTY_STRING_BUFFER(config.formation))
	{
		(void) cli_show_formation_uri(argc, argv);
	}
	else
	{
		(void) cli_show_monitor_uri(argc, argv);
	}
}


/*
 * keeper_cli_formation_uri lists the connection string to connect to a formation
 */
static void
cli_show_formation_uri(int argc, char **argv)
{
	KeeperConfig config = keeperOptions;
	Monitor monitor = { 0 };
	char postgresUri[MAXCONNINFO];

	/* when --formation is missing, use the default value */
	if (IS_EMPTY_STRING_BUFFER(config.formation))
	{
		strlcpy(config.formation, FORMATION_DEFAULT, NAMEDATALEN);
	}

	if (!monitor_init_from_pgsetup(&monitor, &config.pgSetup))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!monitor_formation_uri(&monitor, config.formation, postgresUri, MAXCONNINFO) )
	{
		/* errors have already been logged */
		exit(EXIT_CODE_MONITOR);
	}

	fprintf(stdout, "%s\n", postgresUri);
}


/*
 * keeper_cli_monitor_uri shows the postgres uri to use for connecting to the
 * monitor
 */
static void
cli_show_monitor_uri(int argc, char **argv)
{
	KeeperConfig kconfig = keeperOptions;

	if (!keeper_config_set_pathnames_from_pgdata(&kconfig.pathnames, kconfig.pgSetup.pgdata))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_CONFIG);
	}

	switch (ProbeConfigurationFileRole(kconfig.pathnames.config))
	{
		case PG_AUTOCTL_ROLE_MONITOR:
		{
			Monitor monitor = { 0 };
			MonitorConfig mconfig = { 0 };
			bool missing_pgdata_is_ok = true;
			bool pg_is_not_running_is_ok = true;
			char connInfo[MAXCONNINFO];

			if (!monitor_config_init_from_pgsetup(&monitor,
												  &mconfig,
												  &kconfig.pgSetup,
												  missing_pgdata_is_ok,
												  pg_is_not_running_is_ok))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_PGCTL);
			}

			if (!monitor_config_get_postgres_uri(&mconfig,
												 connInfo,
												 MAXCONNINFO))
			{
				log_fatal("Failed to get postgres connection string");
				exit(EXIT_CODE_BAD_STATE);
			}

			fprintf(stdout, "%s\n", connInfo);

			break;
		}

		case PG_AUTOCTL_ROLE_KEEPER:
		{
			char value[BUFSIZE];

			if (keeper_config_get_setting(&kconfig,
										  "pg_autoctl.monitor",
										  value,
										  BUFSIZE))
			{
				fprintf(stdout, "%s\n", value);
			}
			else
			{
				log_error("Failed to lookup option pg_autoctl.monitor");
				exit(EXIT_CODE_BAD_ARGS);
			}
			break;
		}

		default:
		{
			log_fatal("Unrecognized configuration file \"%s\"",
					  kconfig.pathnames.config);
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
}


/*
 * keeper_show_uri_getopts parses the command line options for the
 * command `pg_autoctl show uri`.
 */
static int
cli_show_files_getopts(int argc, char **argv)
{
	KeeperConfig options = { 0 };
	ShowFilesOptions fileOptions = { 0 };
	int c, option_index = 0;
	int verboseCount = 0;
	int optionCount = 0;

	static struct option long_options[] = {
		{ "pgdata", required_argument, NULL, 'D' },
		{ "config", no_argument, NULL, 'c' },
		{ "state", no_argument, NULL, 's' },
		{ "init", no_argument, NULL, 'i' },
		{ "pid", no_argument, NULL, 'p' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	while ((c = getopt_long(argc, argv, "D:f:Vvqh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'D':
			{
				strlcpy(options.pgSetup.pgdata, optarg, MAXPGPATH);
				log_trace("--pgdata %s", options.pgSetup.pgdata);
				break;
			}

			case 'c':
			{
				if (++optionCount > 1)
				{
					log_error(
						"Please use only one of --config --state --init --pid");
					commandline_help(stderr);
				}
				fileOptions.showConfig = true;
				log_trace("--config");
				break;
			}

			case 's':
			{
				if (++optionCount > 1)
				{
					log_error(
						"Please use only one of --config --state --init --pid");
					commandline_help(stderr);
				}
				fileOptions.showState = true;
				log_trace("--state");
				break;
			}

			case 'i':
			{
				if (++optionCount > 1)
				{
					log_error(
						"Please use only one of --config --state --init --pid");
					commandline_help(stderr);
				}
				fileOptions.showInit = true;
				log_trace("--init");
				break;
			}

			case 'p':
			{
				if (++optionCount > 1)
				{
					log_error(
						"Please use only one of --config --state --init --pid");
					commandline_help(stderr);
				}
				fileOptions.showPid = true;
				log_trace("--pid");
				break;
			}

			case 'V':
			{
				/* keeper_cli_print_version prints version and exits. */
				keeper_cli_print_version(argc, argv);
				break;
			}

			case 'v':
			{
				++verboseCount;
				switch (verboseCount)
				{
					case 1:
						log_set_level(LOG_INFO);
						break;

					case 2:
						fileOptions.showFileContent = true;
						log_set_level(LOG_DEBUG);
						break;

					default:
						fileOptions.showFileContent = true;
						log_set_level(LOG_TRACE);
						break;
				}
				break;
			}

			case 'q':
			{
				log_set_level(LOG_ERROR);
				break;
			}

			case 'h':
			{
				commandline_help(stderr);
				exit(EXIT_CODE_QUIT);
				break;
			}

			default:
			{
				log_error("Failed to parse command line, see above for details.");
				commandline_help(stderr);
				exit(EXIT_CODE_BAD_ARGS);
				break;
			}
		}
	}

	keeperOptions = options;
	showFilesOptions = fileOptions;

	return optind;
}


/*
 * cli_show_files lists the files used by pg_autoctl.
 */
static void
cli_show_files(int argc, char **argv)
{
	KeeperConfig config = keeperOptions;

	if (!keeper_config_set_pathnames_from_pgdata(&config.pathnames,
												 config.pgSetup.pgdata))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_CONFIG);
	}

	switch (ProbeConfigurationFileRole(config.pathnames.config))
	{
		case PG_AUTOCTL_ROLE_MONITOR:
		{
			if (showFilesOptions.showConfig)
			{
				fprintf(stdout, "%s\n", config.pathnames.config);
			}

			if (showFilesOptions.showState)
			{
				log_error("A monitor has no state file");
			}

			if (showFilesOptions.showInit)
			{
				log_error("A monitor has no init state file");
			}

			if (showFilesOptions.showPid)
			{
				log_error("A monitor has no pid file");
			}

			break;
		}

		case PG_AUTOCTL_ROLE_KEEPER:
		{
			if (showFilesOptions.showConfig)
			{
				fprintf(stdout, "%s\n", config.pathnames.config);

				if (showFilesOptions.showFileContent)
				{
					if (!fprint_file_contents(config.pathnames.config))
					{
						/* errors have already been logged */
						exit(EXIT_CODE_BAD_CONFIG);
					}
				}
			}

			if (showFilesOptions.showState)
			{
				fprintf(stdout, "%s\n", config.pathnames.state);

				if (showFilesOptions.showFileContent)
				{
					KeeperStateData keeperState = { 0 };

					if (keeper_state_read(&keeperState, config.pathnames.state))
					{
						(void) print_keeper_state(&keeperState, stdout);
					}
					else
					{
						/* errors have already been logged */
						exit(EXIT_CODE_BAD_STATE);
					}
				}
			}

			if (showFilesOptions.showInit)
			{
				if (file_exists(config.pathnames.init))
				{
					fprintf(stdout, "%s\n", config.pathnames.init);
				}
				else
				{
					log_warn("This keeper does not have an init file at \"%s\"",
							 config.pathnames.init);
				}
			}

			if (showFilesOptions.showPid)
			{
				if (file_exists(config.pathnames.pid))
				{
					fprintf(stdout, "%s\n", config.pathnames.pid);

					if (showFilesOptions.showFileContent)
					{
						if (!fprint_file_contents(config.pathnames.pid))
						{
							/* errors have already been logged */
							exit(EXIT_CODE_INTERNAL_ERROR);
						}
					}
				}
				else
				{
					log_warn("This keeper does not have a pid file at \"%s\"",
							 config.pathnames.pid);
				}
			}

			break;
		}

		default:
		{
			log_fatal("Unrecognized configuration file \"%s\"",
					  config.pathnames.config);
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
}


/*
 * cli_show_maybe_fprint_file_contents prints the content of the given filename
 * to stdout when showFilesOptions.showFileContent is true.
 */
static bool
fprint_file_contents(const char *filename)
{
	char *contents = NULL;
	long size = 0L;

	if (read_file(filename, &contents, &size))
	{
		fprintf(stdout, "%s\n", contents);
		return true;
	}
	else
	{
		/* errors have already been logged */
		return false;
	}
}
