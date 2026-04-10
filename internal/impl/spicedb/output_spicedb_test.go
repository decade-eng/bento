package spicedb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	authzedv1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/warpstreamlabs/bento/public/service"
)

type mockSpiceDBClient struct {
	mu sync.Mutex

	writeRequests []*authzedv1.WriteRelationshipsRequest
	writeResponse *authzedv1.WriteRelationshipsResponse
	writeErr      error
	closeCalled   bool
	closeErr      error
}

func (m *mockSpiceDBClient) WriteRelationships(_ context.Context, req *authzedv1.WriteRelationshipsRequest, _ ...grpc.CallOption) (*authzedv1.WriteRelationshipsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeRequests = append(m.writeRequests, req)
	return m.writeResponse, m.writeErr
}

func (m *mockSpiceDBClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalled = true
	return m.closeErr
}

func (m *mockSpiceDBClient) getWriteRequests() []*authzedv1.WriteRelationshipsRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*authzedv1.WriteRelationshipsRequest, len(m.writeRequests))
	copy(cp, m.writeRequests)
	return cp
}

func newTestWriter(mock *mockSpiceDBClient) *spiceDBWriter {
	resourceType, _ := service.NewInterpolatedString(`${! json("resource_type") }`)
	resourceID, _ := service.NewInterpolatedString(`${! json("resource_id") }`)
	relation, _ := service.NewInterpolatedString(`${! json("relation") }`)
	subjectType, _ := service.NewInterpolatedString(`${! json("subject_type") }`)
	subjectID, _ := service.NewInterpolatedString(`${! json("subject_id") }`)
	subjectRelation, _ := service.NewInterpolatedString("")
	operation, _ := service.NewInterpolatedString("TOUCH")

	return &spiceDBWriter{
		endpoint:        "localhost:50051",
		bearerToken:     "test-token",
		resourceType:    resourceType,
		resourceID:      resourceID,
		relation:        relation,
		subjectType:     subjectType,
		subjectID:       subjectID,
		subjectRelation: subjectRelation,
		operation:       operation,
		client:          mock,
		log:             service.MockResources().Logger(),
	}
}

func newTestMsg(resourceType, resourceID, relation, subjectType, subjectID string) *service.Message {
	payload := fmt.Sprintf(`{"resource_type":%q,"resource_id":%q,"relation":%q,"subject_type":%q,"subject_id":%q}`,
		resourceType, resourceID, relation, subjectType, subjectID)
	return service.NewMessage([]byte(payload))
}

func TestSpiceDBWriterNotConnected(t *testing.T) {
	w := newTestWriter(nil)
	w.client = nil

	batch := service.MessageBatch{newTestMsg("doc", "doc1", "viewer", "user", "alice")}
	err := w.WriteBatch(context.Background(), batch)
	require.ErrorIs(t, err, service.ErrNotConnected)
}

func TestSpiceDBWriterSingleMessage(t *testing.T) {
	mock := &mockSpiceDBClient{
		writeResponse: &authzedv1.WriteRelationshipsResponse{
			WrittenAt: &authzedv1.ZedToken{Token: "test-token"},
		},
	}
	w := newTestWriter(mock)

	batch := service.MessageBatch{newTestMsg("document", "doc1", "viewer", "user", "alice")}
	require.NoError(t, w.WriteBatch(context.Background(), batch))

	reqs := mock.getWriteRequests()
	require.Len(t, reqs, 1)
	require.Len(t, reqs[0].Updates, 1)

	update := reqs[0].Updates[0]
	assert.Equal(t, authzedv1.RelationshipUpdate_OPERATION_TOUCH, update.Operation)
	assert.Equal(t, "document", update.Relationship.Resource.ObjectType)
	assert.Equal(t, "doc1", update.Relationship.Resource.ObjectId)
	assert.Equal(t, "viewer", update.Relationship.Relation)
	assert.Equal(t, "user", update.Relationship.Subject.Object.ObjectType)
	assert.Equal(t, "alice", update.Relationship.Subject.Object.ObjectId)
	assert.Empty(t, update.Relationship.Subject.OptionalRelation)
}

func TestSpiceDBWriterBatchMessages(t *testing.T) {
	mock := &mockSpiceDBClient{
		writeResponse: &authzedv1.WriteRelationshipsResponse{
			WrittenAt: &authzedv1.ZedToken{Token: "tok"},
		},
	}
	w := newTestWriter(mock)

	batch := service.MessageBatch{
		newTestMsg("document", "doc1", "viewer", "user", "alice"),
		newTestMsg("document", "doc2", "editor", "user", "bob"),
		newTestMsg("document", "doc3", "viewer", "user", "carol"),
		newTestMsg("document", "doc4", "viewer", "user", "dan"),
	}
	require.NoError(t, w.WriteBatch(context.Background(), batch))

	reqs := mock.getWriteRequests()
	require.Len(t, reqs, 1, "all 4 updates should go in one call")
	assert.Len(t, reqs[0].Updates, 4)
}

func TestSpiceDBWriterAutoChunk(t *testing.T) {
	mock := &mockSpiceDBClient{
		writeResponse: &authzedv1.WriteRelationshipsResponse{
			WrittenAt: &authzedv1.ZedToken{Token: "tok"},
		},
	}
	w := newTestWriter(mock)

	batch := make(service.MessageBatch, 1500)
	for i := range batch {
		batch[i] = newTestMsg("document", fmt.Sprintf("doc%d", i), "viewer", "user", fmt.Sprintf("user%d", i))
	}

	require.NoError(t, w.WriteBatch(context.Background(), batch))

	reqs := mock.getWriteRequests()
	require.Len(t, reqs, 2, "should be split into 2 calls: 1000 + 500")
	assert.Len(t, reqs[0].Updates, 1000)
	assert.Len(t, reqs[1].Updates, 500)
}

func TestSpiceDBWriterOperationInterpolation(t *testing.T) {
	mock := &mockSpiceDBClient{
		writeResponse: &authzedv1.WriteRelationshipsResponse{
			WrittenAt: &authzedv1.ZedToken{Token: "tok"},
		},
	}

	opField, _ := service.NewInterpolatedString(`${! json("op") }`)
	resourceType, _ := service.NewInterpolatedString("document")
	resourceID, _ := service.NewInterpolatedString(`${! json("resource_id") }`)
	relation, _ := service.NewInterpolatedString("viewer")
	subjectType, _ := service.NewInterpolatedString("user")
	subjectID, _ := service.NewInterpolatedString(`${! json("subject_id") }`)
	subjectRelation, _ := service.NewInterpolatedString("")

	w := &spiceDBWriter{
		resourceType:    resourceType,
		resourceID:      resourceID,
		relation:        relation,
		subjectType:     subjectType,
		subjectID:       subjectID,
		subjectRelation: subjectRelation,
		operation:       opField,
		client:          mock,
		log:             service.MockResources().Logger(),
	}

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"resource_id":"doc1","subject_id":"alice","op":"TOUCH"}`)),
		service.NewMessage([]byte(`{"resource_id":"doc2","subject_id":"bob","op":"CREATE"}`)),
		service.NewMessage([]byte(`{"resource_id":"doc3","subject_id":"carol","op":"DELETE"}`)),
	}

	require.NoError(t, w.WriteBatch(context.Background(), batch))
	reqs := mock.getWriteRequests()
	require.Len(t, reqs, 1)
	require.Len(t, reqs[0].Updates, 3)
	assert.Equal(t, authzedv1.RelationshipUpdate_OPERATION_TOUCH, reqs[0].Updates[0].Operation)
	assert.Equal(t, authzedv1.RelationshipUpdate_OPERATION_CREATE, reqs[0].Updates[1].Operation)
	assert.Equal(t, authzedv1.RelationshipUpdate_OPERATION_DELETE, reqs[0].Updates[2].Operation)
}

func TestSpiceDBWriterUnknownOperation(t *testing.T) {
	mock := &mockSpiceDBClient{
		writeResponse: &authzedv1.WriteRelationshipsResponse{
			WrittenAt: &authzedv1.ZedToken{Token: "tok"},
		},
	}

	opField, _ := service.NewInterpolatedString("INVALID")
	resourceType, _ := service.NewInterpolatedString("document")
	resourceID, _ := service.NewInterpolatedString("doc1")
	relation, _ := service.NewInterpolatedString("viewer")
	subjectType, _ := service.NewInterpolatedString("user")
	subjectID, _ := service.NewInterpolatedString("alice")
	subjectRelation, _ := service.NewInterpolatedString("")

	w := &spiceDBWriter{
		resourceType:    resourceType,
		resourceID:      resourceID,
		relation:        relation,
		subjectType:     subjectType,
		subjectID:       subjectID,
		subjectRelation: subjectRelation,
		operation:       opField,
		client:          mock,
		log:             service.MockResources().Logger(),
	}

	batch := service.MessageBatch{newTestMsg("document", "doc1", "viewer", "user", "alice")}
	err := w.WriteBatch(context.Background(), batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown operation")
}

func TestSpiceDBWriterZedTokenPropagation(t *testing.T) {
	const token = "abc-token-xyz"
	mock := &mockSpiceDBClient{
		writeResponse: &authzedv1.WriteRelationshipsResponse{
			WrittenAt: &authzedv1.ZedToken{Token: token},
		},
	}
	w := newTestWriter(mock)
	w.propagateResponse = true

	batch := service.MessageBatch{newTestMsg("document", "doc1", "viewer", "user", "alice")}
	err := w.WriteBatch(context.Background(), batch)
	require.NoError(t, err)

	reqs := mock.getWriteRequests()
	require.Len(t, reqs, 1)

	expectedPayload := fmt.Sprintf(`{"written_at":%q}`, token)
	assert.Equal(t, `{"written_at":"abc-token-xyz"}`, expectedPayload)
}

func TestSpiceDBWriterZedTokenFormat(t *testing.T) {
	token := "GhUKEzE3MDY4NTMyMDAwMDAwMDAwMA=="
	payload := fmt.Sprintf(`{"written_at":%q}`, token)
	assert.Equal(t, `{"written_at":"GhUKEzE3MDY4NTMyMDAwMDAwMDAwMA=="}`, payload)
}

func TestSpiceDBWriterAPIError(t *testing.T) {
	tests := []struct {
		name    string
		grpcErr error
		wantErr bool
	}{
		{
			name:    "unavailable is returned",
			grpcErr: status.Error(codes.Unavailable, "service unavailable"),
			wantErr: true,
		},
		{
			name:    "invalid argument is returned",
			grpcErr: status.Error(codes.InvalidArgument, "bad relationship"),
			wantErr: true,
		},
		{
			name:    "already exists is returned",
			grpcErr: status.Error(codes.AlreadyExists, "exists"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockSpiceDBClient{writeErr: tt.grpcErr}
			w := newTestWriter(mock)

			batch := service.MessageBatch{newTestMsg("document", "doc1", "viewer", "user", "alice")}
			err := w.WriteBatch(context.Background(), batch)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSpiceDBWriterClose(t *testing.T) {
	mock := &mockSpiceDBClient{}
	w := newTestWriter(mock)

	require.NoError(t, w.Close(context.Background()))
	assert.True(t, mock.closeCalled)
	assert.Nil(t, w.client)
}

func TestSpiceDBWriterConnectIdempotent(t *testing.T) {
	mock := &mockSpiceDBClient{}
	w := newTestWriter(mock)
	err := w.Connect(context.Background())
	require.NoError(t, err)
	assert.Equal(t, mock, w.client)
}

func TestSpiceDBConfigParsing(t *testing.T) {
	spec := spiceDBOutputSpec()

	conf, err := spec.ParseYAML(`
connection:
  endpoint: localhost:50051
  bearer_token: "my-token"
relationship_mapping:
  resource_type: document
  resource_id: doc1
  relation: viewer
  subject_type: user
  subject_id: alice
`, nil)
	require.NoError(t, err)

	w, err := newSpiceDBWriterFromParsed(conf, service.MockResources())
	require.NoError(t, err)
	assert.Equal(t, "localhost:50051", w.endpoint)
	assert.Equal(t, "my-token", w.bearerToken)
	assert.NotNil(t, w.resourceType)
	assert.NotNil(t, w.resourceID)
	assert.NotNil(t, w.relation)
	assert.NotNil(t, w.subjectType)
	assert.NotNil(t, w.subjectID)
}

func TestSpiceDBWriterSubjectRelation(t *testing.T) {
	mock := &mockSpiceDBClient{
		writeResponse: &authzedv1.WriteRelationshipsResponse{
			WrittenAt: &authzedv1.ZedToken{Token: "tok"},
		},
	}

	subjectRelation, _ := service.NewInterpolatedString("member")
	w := newTestWriter(mock)
	w.subjectRelation = subjectRelation

	batch := service.MessageBatch{newTestMsg("group", "eng", "member", "user", "alice")}
	require.NoError(t, w.WriteBatch(context.Background(), batch))

	reqs := mock.getWriteRequests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "member", reqs[0].Updates[0].Relationship.Subject.OptionalRelation)
}
