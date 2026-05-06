#!/usr/bin/env bash

main() {
    set -o pipefail

    local project_root env_file dump_file runtime remote_dump
    project_root="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
    env_file="${project_root}/.env"
    dump_file="${1:-}"

    if [[ -z "${dump_file}" ]]; then
        printf 'Usage: ./load_postgres.sh path/to/dump_file.dump\n' >&2
        return 1
    fi

    if [[ ! -f "${dump_file}" ]]; then
        printf 'Dump file not found: %s\n' "${dump_file}" >&2
        return 1
    fi

    if [[ ! -f "${env_file}" ]]; then
        printf 'Missing .env file at %s\n' "${env_file}" >&2
        return 1
    fi

    set -a
    # shellcheck disable=SC1090
    source "${env_file}"
    set +a

    if [[ -z "${POSTGRES_CONTAINER_NAME:-}" || -z "${POSTGRES_DB:-}" || -z "${POSTGRES_USER:-}" ]]; then
        printf 'POSTGRES_CONTAINER_NAME, POSTGRES_DB and POSTGRES_USER are required in .env\n' >&2
        return 1
    fi

    if command -v podman >/dev/null 2>&1 && podman container exists "${POSTGRES_CONTAINER_NAME}"; then
        runtime="podman"
    elif command -v docker >/dev/null 2>&1 && docker container inspect "${POSTGRES_CONTAINER_NAME}" >/dev/null 2>&1; then
        runtime="docker"
    else
        printf 'Container %s was not found in podman or docker.\n' "${POSTGRES_CONTAINER_NAME}" >&2
        return 1
    fi

    remote_dump="/tmp/$(basename "${dump_file}")"

    printf 'Runtime: %s\n' "${runtime}"
    printf 'Copying dump to container...\n'
    "${runtime}" cp "${dump_file}" "${POSTGRES_CONTAINER_NAME}:${remote_dump}" || return 1

    "${runtime}" exec "${POSTGRES_CONTAINER_NAME}" psql \
        --username "${POSTGRES_USER}" \
        --dbname postgres \
        --command "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${POSTGRES_DB}' AND pid <> pg_backend_pid();" || return 1

    "${runtime}" exec "${POSTGRES_CONTAINER_NAME}" dropdb \
        --username "${POSTGRES_USER}" \
        --if-exists \
        "${POSTGRES_DB}" || return 1

    "${runtime}" exec "${POSTGRES_CONTAINER_NAME}" createdb \
        --username "${POSTGRES_USER}" \
        "${POSTGRES_DB}" || return 1

    "${runtime}" exec "${POSTGRES_CONTAINER_NAME}" pg_restore \
        --username "${POSTGRES_USER}" \
        --dbname "${POSTGRES_DB}" \
        --clean \
        --if-exists \
        --verbose \
        "${remote_dump}" || return 1

    "${runtime}" exec "${POSTGRES_CONTAINER_NAME}" rm -f "${remote_dump}" || return 1

    printf 'Dump loaded into database: %s\n' "${POSTGRES_DB}"
}

main "$@"
script_exit_code=$?

if [[ "${BASH_SOURCE[0]:-$0}" != "$0" ]]; then
    return "${script_exit_code}"
fi

exit "${script_exit_code}"
