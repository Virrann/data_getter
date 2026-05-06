#!/usr/bin/env bash

main() {
    set -o pipefail

    local project_root env_file dump_dir timestamp runtime dump_file log_file
    project_root="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
    env_file="${project_root}/.env"
    dump_dir="${project_root}/data/db_dumps"
    timestamp="$(date +%Y%m%d_%H%M%S)"

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

    mkdir -p "${dump_dir}" || return 1

    if command -v podman >/dev/null 2>&1 && podman container exists "${POSTGRES_CONTAINER_NAME}"; then
        runtime="podman"
    elif command -v docker >/dev/null 2>&1 && docker container inspect "${POSTGRES_CONTAINER_NAME}" >/dev/null 2>&1; then
        runtime="docker"
    else
        printf 'Container %s was not found in podman or docker.\n' "${POSTGRES_CONTAINER_NAME}" >&2
        return 1
    fi

    dump_file="${dump_dir}/${POSTGRES_DB}_${timestamp}.dump"
    log_file="${dump_dir}/${POSTGRES_DB}_${timestamp}.log"

    printf 'Runtime: %s\n' "${runtime}"
    printf 'Creating dump from database %s...\n' "${POSTGRES_DB}"
    printf 'Log: %s\n' "${log_file}"

    if ! "${runtime}" exec "${POSTGRES_CONTAINER_NAME}" pg_dump \
        --username "${POSTGRES_USER}" \
        --dbname "${POSTGRES_DB}" \
        --format custom \
        --blobs \
        > "${dump_file}" \
        2> "${log_file}"; then
        rm -f "${dump_file}"
        printf 'pg_dump failed. See log: %s\n' "${log_file}" >&2
        return 1
    fi

    if [[ ! -s "${dump_file}" ]]; then
        rm -f "${dump_file}"
        printf 'Dump file is missing or empty. See log: %s\n' "${log_file}" >&2
        return 1
    fi

    printf '\nDump completed successfully.\n'
    printf 'Dump saved to: %s\n' "${dump_file}"
    printf 'Log saved to: %s\n' "${log_file}"
    printf 'Size bytes: %s\n' "$(wc -c < "${dump_file}")"
}

main "$@"
script_exit_code=$?

if [[ "${BASH_SOURCE[0]:-$0}" != "$0" ]]; then
    return "${script_exit_code}"
fi

exit "${script_exit_code}"
