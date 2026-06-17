package types

// PreprocessedKnowledgeImportRequest imports chunks that were produced by an
// external preprocessing pipeline. The server persists the chunks directly and
// indexes them without running DocReader or the built-in chunker again.
type PreprocessedKnowledgeImportRequest struct {
	Document PreprocessedKnowledgeDocument `json:"document"`
	Chunks   []PreprocessedChunk           `json:"chunks" binding:"required"`
	Title    string                        `json:"title,omitempty"`
	FileName string                        `json:"file_name,omitempty"`
	TagID    string                        `json:"tag_id,omitempty"`
	Channel  string                        `json:"channel,omitempty"`
}

type PreprocessedKnowledgeDocument struct {
	DocID      string         `json:"doc_id"`
	SourceFile string         `json:"source_file"`
	SourcePath string         `json:"source_path"`
	Title      string         `json:"title,omitempty"`
	DocTitle   string         `json:"doc_title,omitempty"`
	DocType    string         `json:"doc_type"`
	Product    string         `json:"product"`
	Language   string         `json:"language"`
	Status     string         `json:"status"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

type PreprocessedChunk struct {
	ChunkID        string           `json:"chunk_id"`
	DocID          string           `json:"doc_id"`
	SourceFile     string           `json:"source_file"`
	SourcePath     string           `json:"source_path"`
	HeadingPath    string           `json:"heading_path"`
	DocType        string           `json:"doc_type"`
	Product        string           `json:"product"`
	Module         string           `json:"module"`
	Intent         string           `json:"intent"`
	Status         string           `json:"status"`
	Language       string           `json:"language"`
	Title          string           `json:"title"`
	Summary        string           `json:"summary"`
	Keywords       []string         `json:"keywords"`
	Aliases        []string         `json:"aliases"`
	UnitType       string           `json:"unit_type"`
	ParentID       string           `json:"parent_id"`
	GraphEntities  []string         `json:"graph_entities"`
	GraphRelations []map[string]any `json:"graph_relations"`
	Content        string           `json:"content"`
	Metadata       map[string]any   `json:"metadata,omitempty"`
}
