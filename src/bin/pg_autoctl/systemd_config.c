/*
 * src/bin/pg_autoctl/systemd_config.c
 *     Keeper configuration functions
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */

#include <stdbool.h>
#include <unistd.h>

#include "postgres_fe.h"

#include "cli_root.h"
#include "defaults.h"
#include "ini_file.h"
#include "systemd_config.h"
#include "log.h"

#include "runprogram.h"

#define OPTION_SYSTEMD_DESCRIPTION(config) \
	make_strbuf_option_default("Unit", "Description", NULL, true, BUFSIZE, \
							   config->Description, "pg_auto_failover")

#define OPTION_SYSTEMD_WORKING_DIRECTORY(config) \
	make_strbuf_option_default("Service", "WorkingDirectory",			\
							   NULL, true, BUFSIZE,						\
							   config->WorkingDirectory, "/var/lib/postgresql")

#define OPTION_SYSTEMD_ENVIRONMENT_PGDATA(config) \
	make_strbuf_option_default("Service", "Environment",				\
							   NULL, true, BUFSIZE,						\
							   config->EnvironmentPGDATA,				\
							   "PGDATA=/var/lib/postgresql/11/pg_auto_failover")

#define OPTION_SYSTEMD_USER(config) \
	make_strbuf_option_default("Service", "User", NULL, true, BUFSIZE,	\
							   config->User, "postgres")

#define OPTION_SYSTEMD_EXECSTART(config) \
	make_strbuf_option_default("Service", "ExecStart", NULL, true, BUFSIZE,	\
							   config->ExecStart, "/usr/bin/pg_autoctl run")

#define OPTION_SYSTEMD_RESTART(config) \
	make_strbuf_option_default("Service", "Restart", NULL, true, BUFSIZE, \
							   config->Restart, "always")

#define OPTION_SYSTEMD_STARTLIMITBURST(config) \
	make_int_option_default("Service", "StartLimitBurst", NULL, true, \
							&(config->StartLimitBurst), 20)

#define OPTION_SYSTEMD_WANTEDBY(config) \
	make_strbuf_option_default("Install", "WantedBy", NULL, true, BUFSIZE, \
							   config->WantedBy, "multi-user.target")

#define SET_INI_OPTIONS_ARRAY(config) \
	{ \
		OPTION_SYSTEMD_DESCRIPTION(config),		 \
		OPTION_SYSTEMD_WORKING_DIRECTORY(config),	\
		OPTION_SYSTEMD_ENVIRONMENT_PGDATA(config),	\
		OPTION_SYSTEMD_USER(config),				\
		OPTION_SYSTEMD_EXECSTART(config),			\
		OPTION_SYSTEMD_RESTART(config),				\
		OPTION_SYSTEMD_STARTLIMITBURST(config),		\
		OPTION_SYSTEMD_WANTEDBY(config),			\
		INI_OPTION_LAST \
	}


/*
 * systemd_config_init initialises a SystemdServiceConfig with the default
 * values.
 */
void
systemd_config_init(SystemdServiceConfig *config, const char *pgdata)
{
	IniOption systemdOptions[] = SET_INI_OPTIONS_ARRAY(config);

	/* time to setup config->pathnames.systemd */
	snprintf(config->pathnames.systemd, MAXPGPATH,
			 "/etc/systemd/system/%s", KEEPER_SYSTEMD_FILENAME);

	/* adjust defaults to known values from the config */
	strlcpy(config->WorkingDirectory, config->pgSetup.pgdata, MAXPGPATH);

	snprintf(config->EnvironmentPGDATA, BUFSIZE,
			 "'PGDATA=%s'", config->pgSetup.pgdata);

	strlcpy(config->User, config->pgSetup.username, NAMEDATALEN);

	snprintf(config->ExecStart, BUFSIZE, "%s run", pg_autoctl_program);

	if (!ini_validate_options(systemdOptions))
	{
		log_error("Please review your setup options per above messages");
		exit(EXIT_CODE_BAD_CONFIG);
	}
}


/*
 * keeper_config_write write the current config to given STREAM.
 */
bool
systemd_config_write(FILE *stream, SystemdServiceConfig *config)
{
	IniOption systemdOptions[] = SET_INI_OPTIONS_ARRAY(config);

	return write_ini_to_stream(stream, systemdOptions);
}
