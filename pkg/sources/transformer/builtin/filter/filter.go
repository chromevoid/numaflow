/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package filter

import (
	"context"
	"fmt"
	"time"

	"github.com/numaproj/numaflow-go/pkg/sourcetransformer"

	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type filter struct {
	expression string
}

var globalCounter = 0

func New(args map[string]string) (sourcetransformer.SourceTransformFunc, error) {
	expr, existing := args["expression"]
	if !existing {
		return nil, fmt.Errorf(`missing "expression"`)
	}
	f := filter{
		expression: expr,
	}

	return func(ctx context.Context, keys []string, datum sourcetransformer.Datum) sourcetransformer.Messages {
		if globalCounter >= len(testData) {
			globalCounter = 0
		}
		keys = testData[globalCounter]
		globalCounter++
		log := logging.FromContext(ctx)
		resultMsg, err := f.apply(datum.EventTime(), datum.Value())
		if err != nil {
			log.Errorf("Filter map function apply got an error: %v", err)
		}
		return sourcetransformer.MessagesBuilder().Append(resultMsg.WithKeys(keys))
	}, nil
}

func (f filter) apply(et time.Time, msg []byte) (sourcetransformer.Message, error) {
	return sourcetransformer.NewMessage(msg, et), nil
}
