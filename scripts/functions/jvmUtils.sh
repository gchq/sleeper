# Copyright 2022-2026 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
unset CDPATH

# Separates options for the JVM from the arguments for the Java application.
# Sets JVM_OPTIONS to the options to pass to the JVM, and APP_ARGS to the remaining arguments.
# The JVM is always set to exit if it runs out of memory.
#
# Supported options:
#   --jvm-memory <size>  Maximum heap size for the JVM, e.g. 512m or 8g.
#   --force              Skips the confirmation prompt if the requested memory is more than is available.
read_jvm_options() {
  local JVM_MEMORY=""
  local JVM_MEMORY_SET=false
  local FORCE=false
  # Exit as soon as the JVM runs out of memory, rather than continuing with errors that hide the cause
  JVM_OPTIONS=("-XX:+ExitOnOutOfMemoryError")
  APP_ARGS=()
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      --jvm-memory) JVM_MEMORY="${2:-}"; JVM_MEMORY_SET=true; shift; [[ "$#" -gt 0 ]] && shift ;;
      --force) FORCE=true; shift 1 ;;
      *) APP_ARGS+=("$1"); shift 1 ;;
    esac
  done

  if [[ "$JVM_MEMORY_SET" == "true" ]]; then
    check_jvm_memory "$JVM_MEMORY" "$FORCE"
    JVM_OPTIONS+=("-Xmx${JVM_MEMORY}")
  fi
}

# Validates the requested JVM memory, and asks the user to confirm if it is more than is currently available.
# The JVM will usually start anyway, but if it uses that much memory the process may be slowed down by swapping, or
# killed by the operating system without a Java error.
check_jvm_memory() {
  local JVM_MEMORY=$1
  local FORCE=$2
  if [[ -z "$JVM_MEMORY" ]]; then
    echo "Missing value for --jvm-memory"
    exit 1
  fi
  # Match a number then m or g, e.g. 8g, capturing the number in BASH_REMATCH[1] and the unit in BASH_REMATCH[2]
  if [[ ! "$JVM_MEMORY" =~ ^([0-9]+)([mMgG])$ ]]; then
    echo "Invalid value for --jvm-memory: $JVM_MEMORY"
    echo "Expected a whole number followed by m or g, e.g. 512m or 8g"
    exit 1
  fi
  # If force is set or memory info can't be read, return
  if [[ "$FORCE" == "true" || ! -r /proc/meminfo ]]; then
    return 0
  fi

  local NUMBER="${BASH_REMATCH[1]}"
  local REQUESTED_MB
  case ${BASH_REMATCH[2]} in
    m | M) REQUESTED_MB=$((NUMBER)) ;;
    g | G) REQUESTED_MB=$((NUMBER * 1024)) ;;
  esac
  # /proc/meminfo reports values in kB, so convert them to MB
  local AVAILABLE_MB TOTAL_MB
  AVAILABLE_MB=$(awk '/^MemAvailable:/ {print int($2 / 1024)}' /proc/meminfo)
  TOTAL_MB=$(awk '/^MemTotal:/ {print int($2 / 1024)}' /proc/meminfo)
  if [[ -z "$AVAILABLE_MB" || "$REQUESTED_MB" -le "$AVAILABLE_MB" ]]; then
    return 0
  fi

  echo "Requested JVM memory of $JVM_MEMORY is more than the memory currently available" \
    "(${AVAILABLE_MB}m available of ${TOTAL_MB}m total)."
  echo "The process may run slowly, or be killed by the operating system if it uses that much memory."
  if [[ -t 0 ]]; then
    local CONFIRM
    read -r -p "Are you sure you want to continue? [y/N] " CONFIRM
    case $CONFIRM in
      y | Y) ;;
      *) echo "Aborted."; exit 1 ;;
    esac
  fi
}
