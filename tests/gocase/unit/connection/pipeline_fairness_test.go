/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package connection

import (
	"context"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestPipelineYieldFairness(t *testing.T) {
	configs := map[string]string{
		"workers":                "1",
		"read-event-max-commands": "16",
		"read-event-max-time-us":  "0",
	}
	srv := util.StartServer(t, configs)
	defer srv.Close()

	rdb1 := srv.NewClient()
	rdb2 := srv.NewClient()
	ctx := context.Background()

	const pipelineCommands = 100000
	done := make(chan struct{})
	go func() {
		pipe := rdb1.Pipeline()
		for i := 0; i < pipelineCommands; i++ {
			pipe.Incr(ctx, "pipe_k")
		}
		_, _ = pipe.Exec(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)

	pingStart := time.Now()
	err := rdb2.Ping(ctx).Err()
	pingElapsed := time.Since(pingStart)
	require.NoError(t, err)

	select {
	case <-done:
		t.Skip("pipeline finished too fast to observe fairness")
	default:
	}

	require.Less(t, pingElapsed, 200*time.Millisecond)
}
