package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	werrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/google/uuid"
)

const (
	preprocessedImportSource   = "preprocessed_chunks"
	preprocessedImportFileType = "preprocessed"
)

// ImportPreprocessedKnowledge imports chunks that were produced by an external
// preprocessing pipeline. It intentionally bypasses DocReader and WeKnora's
// chunker so caller-provided chunk boundaries and metadata are preserved.
func (s *knowledgeService) ImportPreprocessedKnowledge(
	ctx context.Context,
	kbID string,
	payload *types.PreprocessedKnowledgeImportRequest,
) (*types.Knowledge, error) {
	logger.Info(ctx, "Start importing preprocessed chunks")
	if payload == nil {
		return nil, werrors.NewBadRequestError("request body cannot be empty")
	}
	if len(payload.Chunks) == 0 {
		return nil, werrors.NewBadRequestError("chunks cannot be empty")
	}

	tenantID, ok := types.TenantIDFromContext(ctx)
	if !ok {
		return nil, werrors.NewBadRequestError("tenant id is missing from context")
	}

	kb, err := s.kbService.GetKnowledgeBaseByID(ctx, kbID)
	if err != nil {
		logger.Errorf(ctx, "Failed to get knowledge base: %v", err)
		return nil, err
	}
	if kb.Type == types.KnowledgeBaseTypeFAQ {
		return nil, werrors.NewBadRequestError("FAQ knowledge bases do not support preprocessed chunk import")
	}

	title := firstNonEmpty(payload.Title, payload.Document.Title, payload.Document.DocTitle,
		payload.Document.SourceFile, payload.Document.DocID, "Preprocessed Knowledge")
	safeTitle, valid := secutils.ValidateInput(title)
	if !valid {
		return nil, werrors.NewValidationError("title contains invalid characters or exceeds the length limit")
	}
	fileName := firstNonEmpty(payload.FileName, payload.Document.SourceFile, safeTitle)
	safeFileName, valid := secutils.ValidateInput(fileName)
	if !valid {
		return nil, werrors.NewValidationError("file_name contains invalid characters or exceeds the length limit")
	}

	metadataJSON, err := buildPreprocessedKnowledgeMetadata(payload)
	if err != nil {
		return nil, err
	}

	fileSize := preprocessedContentSize(payload.Chunks)
	fileHash := hashStrings(payload.Document.DocID, payload.Document.SourcePath, fmt.Sprint(fileSize))
	now := time.Now()
	knowledge := &types.Knowledge{
		ID:               uuid.New().String(),
		TenantID:         tenantID,
		KnowledgeBaseID:  kbID,
		TagID:            payload.TagID,
		Type:             "file",
		Source:           preprocessedImportSource,
		Channel:          defaultPreprocessedImportChannel(payload.Channel),
		Title:            safeTitle,
		FileName:         safeFileName,
		FileType:         preprocessedImportFileType,
		FileSize:         fileSize,
		FileHash:         fileHash,
		ParseStatus:      types.ParseStatusProcessing,
		SummaryStatus:    types.SummaryStatusNone,
		EnableStatus:     "disabled",
		CreatedAt:        now,
		UpdatedAt:        now,
		EmbeddingModelID: kb.EmbeddingModelID,
		Metadata:         metadataJSON,
	}

	insertChunks, err := buildPreprocessedChunks(knowledge, payload.Chunks)
	if err != nil {
		return nil, err
	}

	var (
		embeddingModel   embedding.Embedder
		retrieveEngine   *retriever.CompositeRetrieveEngine
		indexInfoList    []*types.IndexInfo
		totalStorageSize int64
	)
	if kb.NeedsEmbeddingModel() {
		embeddingModel, err = s.modelService.GetEmbeddingModel(ctx, kb.EmbeddingModelID)
		if err != nil {
			return nil, err
		}
		tenantInfo, ok := types.TenantInfoFromContext(ctx)
		if !ok {
			return nil, werrors.NewBadRequestError("tenant info is missing from context")
		}
		retrieveEngine, err = retriever.CreateRetrieveEngineForKB(
			ctx, s.retrieveEngine, s.ownership, tenantInfo.ID, kb.VectorStoreID)
		if err != nil {
			return nil, err
		}
		indexInfoList = buildPreprocessedIndexInfo(knowledge, insertChunks)
		totalStorageSize = retrieveEngine.EstimateStorageSize(ctx, embeddingModel, indexInfoList)
		if tenantInfo.StorageQuota > 0 {
			freshTenantInfo, tenantErr := s.tenantRepo.GetTenantByID(ctx, tenantInfo.ID)
			if tenantErr != nil {
				return nil, tenantErr
			}
			if freshTenantInfo.StorageUsed+totalStorageSize > freshTenantInfo.StorageQuota {
				return nil, types.NewStorageQuotaExceededError()
			}
		}
	}

	if err := s.repo.CreateKnowledge(ctx, knowledge); err != nil {
		logger.Errorf(ctx, "Failed to create preprocessed knowledge record: %v", err)
		return nil, err
	}
	cleanup := func() {
		if err := s.chunkService.DeleteChunksByKnowledgeID(ctx, knowledge.ID); err != nil {
			logger.Warnf(ctx, "Failed to cleanup imported chunks for %s: %v", knowledge.ID, err)
		}
		if retrieveEngine != nil && embeddingModel != nil {
			if err := retrieveEngine.DeleteByKnowledgeIDList(
				ctx, []string{knowledge.ID}, embeddingModel.GetDimensions(), kb.Type,
			); err != nil {
				logger.Warnf(ctx, "Failed to cleanup imported indices for %s: %v", knowledge.ID, err)
			}
		}
		if err := s.repo.DeleteKnowledge(ctx, tenantID, knowledge.ID); err != nil {
			logger.Warnf(ctx, "Failed to cleanup imported knowledge %s: %v", knowledge.ID, err)
		}
	}

	if err := s.chunkService.CreateChunks(ctx, insertChunks); err != nil {
		cleanup()
		return nil, err
	}
	if retrieveEngine != nil && embeddingModel != nil {
		if err := retrieveEngine.BatchIndex(ctx, embeddingModel, indexInfoList); err != nil {
			cleanup()
			return nil, err
		}
	}

	now = time.Now()
	knowledge.ParseStatus = types.ParseStatusCompleted
	knowledge.EnableStatus = "enabled"
	knowledge.StorageSize = totalStorageSize
	knowledge.ProcessedAt = &now
	knowledge.UpdatedAt = now
	if err := s.repo.UpdateKnowledge(ctx, knowledge); err != nil {
		cleanup()
		return nil, err
	}

	if totalStorageSize > 0 {
		if tenantInfo, ok := types.TenantInfoFromContext(ctx); ok {
			if err := s.tenantRepo.AdjustStorageUsed(ctx, tenantInfo.ID, totalStorageSize); err != nil {
				logger.Warnf(ctx, "Failed to adjust tenant storage after preprocessed import: %v", err)
			}
		}
	}

	logger.Infof(ctx, "Imported preprocessed knowledge %s with %d chunks", knowledge.ID, len(insertChunks))
	return knowledge, nil
}

func buildPreprocessedKnowledgeMetadata(payload *types.PreprocessedKnowledgeImportRequest) (types.JSON, error) {
	metadata := map[string]any{
		"import_type":       preprocessedImportSource,
		"doc_id":            payload.Document.DocID,
		"preprocess_doc_id": payload.Document.DocID,
		"source_file":       payload.Document.SourceFile,
		"source_path":       payload.Document.SourcePath,
		"doc_type":          payload.Document.DocType,
		"product":           payload.Document.Product,
		"language":          payload.Document.Language,
		"status":            payload.Document.Status,
	}
	if len(payload.Document.Metadata) > 0 {
		metadata["preprocess_metadata"] = payload.Document.Metadata
	}
	return marshalTypesJSON(metadata)
}

func buildPreprocessedChunks(
	knowledge *types.Knowledge,
	sourceChunks []types.PreprocessedChunk,
) ([]*types.Chunk, error) {
	chunks := make([]*types.Chunk, 0, len(sourceChunks))
	offset := 0
	for index, source := range sourceChunks {
		content := strings.TrimSpace(source.Content)
		if content == "" {
			continue
		}
		metadata, err := buildPreprocessedChunkMetadata(source)
		if err != nil {
			return nil, err
		}
		end := offset + len([]rune(content))
		chunk := &types.Chunk{
			ID:              uuid.New().String(),
			TenantID:        knowledge.TenantID,
			KnowledgeID:     knowledge.ID,
			KnowledgeBaseID: knowledge.KnowledgeBaseID,
			TagID:           knowledge.TagID,
			Content:         content,
			ContextHeader:   source.HeadingPath,
			ChunkIndex:      index,
			IsEnabled:       true,
			Status:          int(types.ChunkStatusDefault),
			StartAt:         offset,
			EndAt:           end,
			ChunkType:       types.ChunkTypeText,
			Metadata:        metadata,
			ContentHash:     hashStrings(content),
			CreatedAt:       knowledge.CreatedAt,
			UpdatedAt:       knowledge.UpdatedAt,
		}
		if len(chunks) > 0 {
			previous := chunks[len(chunks)-1]
			previous.NextChunkID = chunk.ID
			chunk.PreChunkID = previous.ID
		}
		chunks = append(chunks, chunk)
		offset = end + 1
	}
	if len(chunks) == 0 {
		return nil, werrors.NewBadRequestError("all chunks are empty")
	}
	return chunks, nil
}

func buildPreprocessedChunkMetadata(source types.PreprocessedChunk) (types.JSON, error) {
	metadata := map[string]any{
		"import_type":       preprocessedImportSource,
		"chunk_id":          source.ChunkID,
		"doc_id":            source.DocID,
		"external_chunk_id": source.ChunkID,
		"external_doc_id":   source.DocID,
		"source_file":       source.SourceFile,
		"source_path":       source.SourcePath,
		"heading_path":      source.HeadingPath,
		"doc_type":          source.DocType,
		"product":           source.Product,
		"module":            source.Module,
		"intent":            source.Intent,
		"status":            source.Status,
		"language":          source.Language,
		"title":             source.Title,
		"summary":           source.Summary,
		"keywords":          source.Keywords,
		"aliases":           source.Aliases,
		"unit_type":         source.UnitType,
		"parent_id":         source.ParentID,
		"graph_entities":    source.GraphEntities,
		"graph_relations":   source.GraphRelations,
	}
	if len(source.Metadata) > 0 {
		metadata["preprocess_metadata"] = source.Metadata
	}
	return marshalTypesJSON(metadata)
}

func buildPreprocessedIndexInfo(knowledge *types.Knowledge, chunks []*types.Chunk) []*types.IndexInfo {
	indexInfoList := make([]*types.IndexInfo, 0, len(chunks))
	for _, chunk := range chunks {
		indexInfoList = append(indexInfoList, &types.IndexInfo{
			Content:         buildPreprocessedIndexContent(knowledge.Title, chunk),
			SourceID:        chunk.ID,
			SourceType:      types.ChunkSourceType,
			ChunkID:         chunk.ID,
			KnowledgeID:     knowledge.ID,
			KnowledgeBaseID: knowledge.KnowledgeBaseID,
			IsEnabled:       true,
		})
	}
	return indexInfoList
}

func buildPreprocessedIndexContent(documentTitle string, chunk *types.Chunk) string {
	parts := make([]string, 0, 6)
	appendPart := func(value string) {
		value = strings.TrimSpace(value)
		if value != "" {
			parts = append(parts, value)
		}
	}
	appendPart(documentTitle)

	var metadata map[string]any
	if err := json.Unmarshal(chunk.Metadata, &metadata); err == nil {
		appendPart(stringFromAny(metadata["title"]))
		appendPart(stringFromAny(metadata["heading_path"]))
		appendPart(stringFromAny(metadata["summary"]))
		if keywords := stringSliceFromAny(metadata["keywords"]); len(keywords) > 0 {
			appendPart("Keywords: " + strings.Join(keywords, ", "))
		}
	}
	appendPart(chunk.Content)
	return strings.Join(parts, "\n\n")
}

func preprocessedContentSize(chunks []types.PreprocessedChunk) int64 {
	var total int64
	for _, chunk := range chunks {
		total += int64(len([]byte(chunk.Content)))
	}
	return total
}

func defaultPreprocessedImportChannel(channel string) string {
	if strings.TrimSpace(channel) == "" {
		return types.ChannelAPI
	}
	return channel
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func hashStrings(values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		hash.Write([]byte(value))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func marshalTypesJSON(value any) (types.JSON, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return types.JSON(data), nil
}

func stringFromAny(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func stringSliceFromAny(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
			result = append(result, strings.TrimSpace(text))
		}
	}
	return result
}
