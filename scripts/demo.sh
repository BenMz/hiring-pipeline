#!/usr/bin/env bash
# End-to-end demo against the compose stack (`make up`). Needs grpcurl and curl.
set -euo pipefail

CMD=localhost:50051
QRY=localhost:50052
MAIL=http://localhost:8025
C=pipeline.v1.PipelineCommandService
Q=pipeline.v1.PipelineQueryService
RUN="demo-$(date +%s)"   # fresh idempotency keys on every run

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
call() { grpcurl -plaintext -d "$3" "$1" "$2" 2>&1; }
field() { sed -n "s/.*\"$1\": \"\{0,1\}\([^\",]*\)\"\{0,1\}.*/\1/p" | head -1; }

step "Open a job"
out=$(call $CMD $C/OpenJob "{\"idempotency_key\":\"$RUN-job\",\"company_id\":\"acme\",\"title\":\"Backend Engineer\"}")
echo "$out"; JOB=$(echo "$out" | field jobId)

step "Two candidates apply"
out=$(call $CMD $C/SubmitApplication "{\"idempotency_key\":\"$RUN-ada\",\"job_id\":\"$JOB\",\"candidate_name\":\"Ada Lovelace\",\"candidate_email\":\"ada.$RUN@example.com\"}")
echo "$out"; ADA=$(echo "$out" | field applicationId)
out=$(call $CMD $C/SubmitApplication "{\"idempotency_key\":\"$RUN-alan\",\"job_id\":\"$JOB\",\"candidate_name\":\"Alan Turing\",\"candidate_email\":\"alan.$RUN@example.com\"}")
echo "$out"; ALAN=$(echo "$out" | field applicationId)

step "Move Ada to screening, then retry the same request (same key: no new event, same answer)"
call $CMD $C/MoveStage "{\"idempotency_key\":\"$RUN-ada-screen\",\"application_id\":\"$ADA\",\"to_stage\":\"STAGE_SCREENING\"}"
call $CMD $C/MoveStage "{\"idempotency_key\":\"$RUN-ada-screen\",\"application_id\":\"$ADA\",\"to_stage\":\"STAGE_SCREENING\"}"

step "Try to skip Ada straight to an offer (not allowed)"
call $CMD $C/MoveStage "{\"idempotency_key\":\"$RUN-ada-skip\",\"application_id\":\"$ADA\",\"to_stage\":\"STAGE_OFFER\"}" || true

step "Move Ada to interview; reject Alan"
call $CMD $C/MoveStage "{\"idempotency_key\":\"$RUN-ada-int\",\"application_id\":\"$ADA\",\"to_stage\":\"STAGE_INTERVIEW\"}"
call $CMD $C/RejectCandidate "{\"idempotency_key\":\"$RUN-alan-rej\",\"application_id\":\"$ALAN\",\"reason\":\"internal note: not emailed\"}"

sleep 1 # the read side is eventually consistent

step "Funnel for the job (query service, from PostgreSQL read models)"
call $QRY $Q/GetFunnel "{\"job_id\":\"$JOB\"}"

step "Applications in interview"
call $QRY $Q/ListApplications "{\"job_id\":\"$JOB\",\"stage\":\"STAGE_INTERVIEW\"}"

step "Emails the notifier sent (Mailpit, $MAIL)"
emails() {
  curl -s "$MAIL/api/v1/search?query=$RUN" |
    grep -o '"Address":"[^"]*@example.com"\|"Subject":"[^"]*"' |
    paste - - | sed 's/"Address":"\([^"]*\)"[[:space:]]*"Subject":"\([^"]*\)"/  \1  ->  \2/' | sort || true
}
for _ in $(seq 20); do [ "$(emails | wc -l)" -ge 5 ] && break; sleep 0.5; done
emails
echo
echo "Expected 5: one per application, Ada's screening and interview, Alan's"
echo "rejection (without the internal reason). The retried move sent nothing."
echo "Read them at $MAIL."
