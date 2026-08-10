// Copyright (c) 2026 SingleStore, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package singlestore

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestTableColumnsMatchSchema(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	assert.True(t, tableColumnsMatchSchema([]string{"id", "value"}, schema))
	assert.False(t, tableColumnsMatchSchema([]string{"id"}, schema))
	assert.False(t, tableColumnsMatchSchema([]string{"id", "value", "extra"}, schema))
	assert.False(t, tableColumnsMatchSchema([]string{"value", "id"}, schema))
	assert.False(t, tableColumnsMatchSchema([]string{"id", "VALUE"}, schema))

	differentTypes := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.Binary},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	assert.True(t, tableColumnsMatchSchema([]string{"id", "value"}, differentTypes))
}
