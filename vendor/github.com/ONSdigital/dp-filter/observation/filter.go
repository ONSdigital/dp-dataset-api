package observation

// Boolean indicators for publish flag
var (
	Published   = true
	Unpublished = false
)

// Filter represents a structure for a filter job
type Filter struct {
	FilterID         string             `json:"filter_id,omitempty"`
	InstanceID       string             `json:"instance_id"`
	DimensionFilters []*DimensionFilter `json:"dimensions,omitempty"`
	Published        *bool              `json:"published,omitempty"`
	Downloads        *Downloads         `json:"downloads,omitempty"`
}

// DimensionFilter represents an object containing a list of dimension values and the dimension name
type DimensionFilter struct {
	Name    string   `json:"name,omitempty"`
	Options []string `json:"options,omitempty"`
}

// Downloads represent a list of download types
type Downloads struct {
	CSV *DownloadItem `json:"csv,omitempty"`
	XLS *DownloadItem `json:"xls,omitempty"`
}

// DownloadItem represents an object containing download details
type DownloadItem struct {
	HRef    string `json:"href,omitempty"`
	Private string `json:"private,omitempty"`
	Public  string `json:"public,omitempty"`
	Size    string `json:"size,omitempty"`
}

// IsEmpty return true if DimensionFilters is nil, empty or contains only empty values
func (f Filter) IsEmpty() bool {
	if len(f.DimensionFilters) == 0 {
		return true
	}

	for _, o := range f.DimensionFilters {
		if o.Name != "" && len(o.Options) > 0 {
			// return at the first non empty option
			return false
		}
	}

	return true
}
