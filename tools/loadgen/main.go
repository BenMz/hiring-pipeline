// Command loadgen writes a realistic event history through the command API,
// for measuring replay. Each successful command appends exactly one event, so
// the event count is the count of successful commands.
//
//	go run ./tools/loadgen -events 100000
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "command service address")
	target := flag.Int64("events", 100_000, "stop after this many events")
	jobs := flag.Int("jobs", 50, "jobs to spread applications over")
	workers := flag.Int("concurrency", 32, "concurrent clients")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	c := pipelinev1.NewPipelineCommandServiceClient(conn)
	ctx := context.Background()
	run := fmt.Sprintf("load-%d", time.Now().UnixNano())

	var events atomic.Int64
	jobIDs := make([]string, *jobs)
	for i := range jobIDs {
		res, err := c.OpenJob(ctx, &pipelinev1.OpenJobRequest{
			IdempotencyKey: fmt.Sprintf("%s-job-%d", run, i),
			CompanyId:      "acme",
			Title:          fmt.Sprintf("Role %d", i),
		})
		if err != nil {
			log.Fatalf("open job: %v", err)
		}
		jobIDs[i] = res.GetJobId()
		events.Add(1)
	}

	start := time.Now()
	var wg sync.WaitGroup
	var apps atomic.Int64
	for w := range *workers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), uint64(start.UnixNano())))
			for events.Load() < *target {
				n := apps.Add(1)
				if err := oneApplication(ctx, c, rng, fmt.Sprintf("%s-app-%d", run, n), jobIDs[rng.IntN(len(jobIDs))], &events); err != nil {
					log.Fatal(err)
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("wrote %d events (%d jobs, %d applications) in %s: %.0f events/s\n",
		events.Load(), *jobs, apps.Load(), elapsed.Round(time.Millisecond), float64(events.Load())/elapsed.Seconds())
}

// oneApplication submits an application and walks it a random distance
// through the funnel. At each stage it stops with probability 0.35, half of
// those as a rejection, so about 18% of applications reach HIRED.
func oneApplication(ctx context.Context, c pipelinev1.PipelineCommandServiceClient, rng *rand.Rand, key, jobID string, events *atomic.Int64) error {
	res, err := c.SubmitApplication(ctx, &pipelinev1.SubmitApplicationRequest{
		IdempotencyKey: key, JobId: jobID,
		CandidateName: "Load Test", CandidateEmail: key + "@example.com",
	})
	if err != nil {
		return fmt.Errorf("submit: %w", err)
	}
	events.Add(1)
	id := res.GetApplicationId()

	stages := []pipelinev1.Stage{
		pipelinev1.Stage_STAGE_SCREENING, pipelinev1.Stage_STAGE_INTERVIEW,
		pipelinev1.Stage_STAGE_OFFER, pipelinev1.Stage_STAGE_HIRED,
	}
	for i, to := range stages {
		if rng.Float64() < 0.35 { // drop out here
			if rng.Float64() < 0.5 {
				return nil // left in this stage
			}
			_, err := c.RejectCandidate(ctx, &pipelinev1.RejectCandidateRequest{
				IdempotencyKey: fmt.Sprintf("%s-rej", key), ApplicationId: id, Reason: "load test",
			})
			if err != nil {
				return fmt.Errorf("reject: %w", err)
			}
			events.Add(1)
			return nil
		}
		_, err := c.MoveStage(ctx, &pipelinev1.MoveStageRequest{
			IdempotencyKey: fmt.Sprintf("%s-move-%d", key, i), ApplicationId: id, ToStage: to,
		})
		if err != nil {
			return fmt.Errorf("move: %w", err)
		}
		events.Add(1)
	}
	return nil
}
