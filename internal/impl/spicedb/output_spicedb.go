package spicedb

import (
	"context"
	"crypto/tls"
	"fmt"

	authzedv1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	authzed "github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	sdbFieldConnection  = "connection"
	sdbFieldEndpoint    = "endpoint"
	sdbFieldBearerToken = "bearer_token"
	sdbFieldTLS         = "tls"

	sdbFieldRelMapping      = "relationship_mapping"
	sdbFieldResourceType    = "resource_type"
	sdbFieldResourceID      = "resource_id"
	sdbFieldRelation        = "relation"
	sdbFieldSubjectType     = "subject_type"
	sdbFieldSubjectID       = "subject_id"
	sdbFieldSubjectRelation = "subject_relation"
	sdbFieldOperation       = "operation"

	sdbFieldPropagateResponse = "propagate_response"
	sdbFieldMaxInFlight       = "max_in_flight"
	sdbFieldBatching          = "batching"

	opTOUCH  = "TOUCH"
	opCREATE = "CREATE"
	opDELETE = "DELETE"
)

type spiceDBClient interface {
	WriteRelationships(ctx context.Context, req *authzedv1.WriteRelationshipsRequest, opts ...grpc.CallOption) (*authzedv1.WriteRelationshipsResponse, error)
	Close() error
}

func spiceDBOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Writes relationship updates to a SpiceDB instance.").
		Categories("services").
		Fields(
			service.NewObjectField(sdbFieldConnection,
				service.NewStringField(sdbFieldEndpoint).
					Description("SpiceDB gRPC endpoint address.").
					Example("localhost:50051"),
				service.NewStringField(sdbFieldBearerToken).
					Description("Bearer token for authentication.").
					Secret(),
				service.NewTLSToggledField(sdbFieldTLS),
			),
			service.NewObjectField(sdbFieldRelMapping,
				service.NewInterpolatedStringField(sdbFieldResourceType).
					Description("The resource object type.").
					Example("document"),
				service.NewInterpolatedStringField(sdbFieldResourceID).
					Description("The resource object ID.").
					Example(`${! json("doc_id") }`),
				service.NewInterpolatedStringField(sdbFieldRelation).
					Description("The relation name.").
					Example("viewer"),
				service.NewInterpolatedStringField(sdbFieldSubjectType).
					Description("The subject object type.").
					Example("user"),
				service.NewInterpolatedStringField(sdbFieldSubjectID).
					Description("The subject object ID.").
					Example(`${! json("user_id") }`),
				service.NewInterpolatedStringField(sdbFieldSubjectRelation).
					Description(`Optional sub-relation on the subject (e.g. "member" for group:eng#member).`).
					Default(""),
				service.NewInterpolatedStringField(sdbFieldOperation).
					Description("The relationship update operation: TOUCH (upsert), CREATE (fail if exists), or DELETE (no-op if missing). Defaults to TOUCH.").
					Default(opTOUCH),
			),
			service.NewBoolField(sdbFieldPropagateResponse).
				Description("Whether to propagate the ZedToken returned by SpiceDB back to the input as a sync response.").
				Default(false),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(sdbFieldBatching),
		)
}

type spiceDBWriter struct {
	endpoint    string
	bearerToken string
	tlsEnabled  bool
	tlsConf     *tls.Config

	resourceType    *service.InterpolatedString
	resourceID      *service.InterpolatedString
	relation        *service.InterpolatedString
	subjectType     *service.InterpolatedString
	subjectID       *service.InterpolatedString
	subjectRelation *service.InterpolatedString
	operation       *service.InterpolatedString

	propagateResponse bool

	client spiceDBClient

	log *service.Logger
}

func newSpiceDBWriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*spiceDBWriter, error) {
	connConf := conf.Namespace(sdbFieldConnection)

	endpoint, err := connConf.FieldString(sdbFieldEndpoint)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldEndpoint, err)
	}

	bearerToken, err := connConf.FieldString(sdbFieldBearerToken)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldBearerToken, err)
	}

	tlsConf, tlsEnabled, err := connConf.FieldTLSToggled(sdbFieldTLS)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldTLS, err)
	}

	relConf := conf.Namespace(sdbFieldRelMapping)

	resourceType, err := relConf.FieldInterpolatedString(sdbFieldResourceType)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldResourceType, err)
	}

	resourceID, err := relConf.FieldInterpolatedString(sdbFieldResourceID)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldResourceID, err)
	}

	relation, err := relConf.FieldInterpolatedString(sdbFieldRelation)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldRelation, err)
	}

	subjectType, err := relConf.FieldInterpolatedString(sdbFieldSubjectType)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldSubjectType, err)
	}

	subjectID, err := relConf.FieldInterpolatedString(sdbFieldSubjectID)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldSubjectID, err)
	}

	subjectRelation, err := relConf.FieldInterpolatedString(sdbFieldSubjectRelation)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldSubjectRelation, err)
	}

	operation, err := relConf.FieldInterpolatedString(sdbFieldOperation)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldOperation, err)
	}

	propagateResponse, err := conf.FieldBool(sdbFieldPropagateResponse)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", sdbFieldPropagateResponse, err)
	}

	return &spiceDBWriter{
		endpoint:          endpoint,
		bearerToken:       bearerToken,
		tlsEnabled:        tlsEnabled,
		tlsConf:           tlsConf,
		resourceType:      resourceType,
		resourceID:        resourceID,
		relation:          relation,
		subjectType:       subjectType,
		subjectID:         subjectID,
		subjectRelation:   subjectRelation,
		operation:         operation,
		propagateResponse: propagateResponse,
		log:               mgr.Logger(),
	}, nil
}

func (w *spiceDBWriter) Connect(_ context.Context) error {
	if w.client != nil {
		return nil
	}

	var opts []grpc.DialOption
	if w.tlsEnabled {
		if w.tlsConf != nil {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(w.tlsConf)))
		} else {
			tlsOpt, err := grpcutil.WithSystemCerts(grpcutil.VerifyCA)
			if err != nil {
				return fmt.Errorf("setting up TLS: %w", err)
			}
			opts = append(opts, tlsOpt)
		}
		opts = append(opts, grpcutil.WithBearerToken(w.bearerToken))
	} else {
		opts = append(opts,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpcutil.WithInsecureBearerToken(w.bearerToken),
		)
	}

	client, err := authzed.NewClient(w.endpoint, opts...)
	if err != nil {
		return fmt.Errorf("connecting to SpiceDB: %w", err)
	}
	w.client = client
	return nil
}

func (w *spiceDBWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if w.client == nil {
		return service.ErrNotConnected
	}

	var batchErr *service.BatchError
	failMsg := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(batch, err)
		}
		batchErr.Failed(i, err)
	}

	var updates []*authzedv1.RelationshipUpdate
	for i := range batch {
		update, err := w.resolveUpdate(batch, i)
		if err != nil {
			failMsg(i, err)
			continue
		}
		updates = append(updates, update)
	}

	var lastResp *authzedv1.WriteRelationshipsResponse
	const maxChunk = 1000
	for start := 0; start < len(updates); start += maxChunk {
		end := min(start+maxChunk, len(updates))
		resp, err := w.client.WriteRelationships(ctx, &authzedv1.WriteRelationshipsRequest{
			Updates: updates[start:end],
		})
		if err != nil {
			w.classifyAndLogError(err)
			return err
		}
		lastResp = resp
	}

	if w.propagateResponse && lastResp != nil && lastResp.WrittenAt != nil {
		payload := fmt.Sprintf(`{"written_at":%q}`, lastResp.WrittenAt.Token)
		responseMsg := batch[0].Copy()
		responseMsg.SetBytes([]byte(payload))
		if err := (service.MessageBatch{responseMsg}).AddSyncResponse(); err != nil {
			w.log.Warnf("Failed to propagate ZedToken sync response: %v", err)
		}
	}

	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}
	return nil
}

func (w *spiceDBWriter) Close(_ context.Context) error {
	if w.client == nil {
		return nil
	}
	err := w.client.Close()
	w.client = nil
	return err
}

func (w *spiceDBWriter) resolveUpdate(batch service.MessageBatch, i int) (*authzedv1.RelationshipUpdate, error) {
	resourceType, err := batch.TryInterpolatedString(i, w.resourceType)
	if err != nil {
		return nil, fmt.Errorf("resource_type: %w", err)
	}
	resourceID, err := batch.TryInterpolatedString(i, w.resourceID)
	if err != nil {
		return nil, fmt.Errorf("resource_id: %w", err)
	}
	relation, err := batch.TryInterpolatedString(i, w.relation)
	if err != nil {
		return nil, fmt.Errorf("relation: %w", err)
	}
	subjectType, err := batch.TryInterpolatedString(i, w.subjectType)
	if err != nil {
		return nil, fmt.Errorf("subject_type: %w", err)
	}
	subjectID, err := batch.TryInterpolatedString(i, w.subjectID)
	if err != nil {
		return nil, fmt.Errorf("subject_id: %w", err)
	}
	subjectRelation, err := batch.TryInterpolatedString(i, w.subjectRelation)
	if err != nil {
		return nil, fmt.Errorf("subject_relation: %w", err)
	}
	opStr, err := batch.TryInterpolatedString(i, w.operation)
	if err != nil {
		return nil, fmt.Errorf("operation: %w", err)
	}
	op, err := parseOperation(opStr)
	if err != nil {
		return nil, err
	}

	return &authzedv1.RelationshipUpdate{
		Operation: op,
		Relationship: &authzedv1.Relationship{
			Resource: &authzedv1.ObjectReference{
				ObjectType: resourceType,
				ObjectId:   resourceID,
			},
			Relation: relation,
			Subject: &authzedv1.SubjectReference{
				Object: &authzedv1.ObjectReference{
					ObjectType: subjectType,
					ObjectId:   subjectID,
				},
				OptionalRelation: subjectRelation,
			},
		},
	}, nil
}

func parseOperation(s string) (authzedv1.RelationshipUpdate_Operation, error) {
	switch s {
	case opTOUCH, "":
		return authzedv1.RelationshipUpdate_OPERATION_TOUCH, nil
	case opCREATE:
		return authzedv1.RelationshipUpdate_OPERATION_CREATE, nil
	case opDELETE:
		return authzedv1.RelationshipUpdate_OPERATION_DELETE, nil
	default:
		return authzedv1.RelationshipUpdate_OPERATION_UNSPECIFIED,
			fmt.Errorf("unknown operation %q: must be TOUCH, CREATE, or DELETE", s)
	}
}

func (w *spiceDBWriter) classifyAndLogError(err error) {
	st, ok := status.FromError(err)
	if !ok {
		return
	}
	switch st.Code() {
	case codes.Unauthenticated, codes.PermissionDenied:
		w.log.Errorf("SpiceDB auth error (%v): %v", st.Code(), st.Message())
	case codes.InvalidArgument, codes.NotFound, codes.FailedPrecondition, codes.AlreadyExists:
		w.log.Warnf("SpiceDB permanent error (%v): %v", st.Code(), st.Message())
	}
}

func init() {
	err := service.RegisterBatchOutput(
		"spicedb", spiceDBOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			maxInFlight, err := conf.FieldInt(sdbFieldMaxInFlight)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			bp, err := conf.FieldBatchPolicy(sdbFieldBatching)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			w, err := newSpiceDBWriterFromParsed(conf, mgr)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			return w, bp, maxInFlight, nil
		},
	)
	if err != nil {
		panic(err)
	}
}
