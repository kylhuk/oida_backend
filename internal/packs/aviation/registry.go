package aviation

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"global-osint-backend/internal/canonical"
)

func DecodeRegistryCSV(r io.Reader) ([]RegistryRecord, error) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	header := make(map[string]int, len(rows[0]))
	for idx, name := range rows[0] {
		header[normalizeHeader(name)] = idx
	}
	required := []string{"nnumber", "modescodehex", "registrantname"}
	for _, name := range required {
		if _, ok := header[name]; !ok {
			return nil, fmt.Errorf("registry header missing %q", name)
		}
	}
	out := make([]RegistryRecord, 0, len(rows)-1)
	for rowIdx, row := range rows[1:] {
		if len(strings.Join(row, "")) == 0 {
			continue
		}
		record := RegistryRecord{
			Registration:   strings.TrimSpace(valueAt(row, header, "nnumber")),
			ModeSCodeHex:   normalizeICAO24(valueAt(row, header, "modescodehex")),
			SerialNumber:   strings.TrimSpace(valueAt(row, header, "serialnumber")),
			RegistrantName: strings.TrimSpace(valueAt(row, header, "registrantname")),
			RegistrantType: strings.TrimSpace(valueAt(row, header, "registranttype")),
			Manufacturer:   strings.TrimSpace(valueAt(row, header, "manufacturer")),
			Model:          strings.TrimSpace(valueAt(row, header, "model")),
			AircraftType:   strings.TrimSpace(valueAt(row, header, "aircrafttype")),
			EngineType:     strings.TrimSpace(valueAt(row, header, "enginetype")),
			CountryCode:    strings.TrimSpace(valueAt(row, header, "countrycode")),
		}
		if yearText := strings.TrimSpace(valueAt(row, header, "year")); yearText != "" {
			year, err := strconv.Atoi(yearText)
			if err != nil {
				return nil, fmt.Errorf("registry row %d invalid year %q", rowIdx+2, yearText)
			}
			record.Year = year
		}
		if record.ModeSCodeHex == "" {
			return nil, fmt.Errorf("registry row %d missing modescodehex", rowIdx+2)
		}
		record.Evidence = []canonical.Evidence{{
			Kind:     "registry_field",
			SourceID: "faa:releasable-aircraft",
			Ref:      record.Registration,
			Value:    record.RegistrantName,
			Attrs: map[string]any{
				"mode_s_code_hex": record.ModeSCodeHex,
				"manufacturer":    record.Manufacturer,
				"model":           record.Model,
			},
		}}
		out = append(out, record)
	}
	return out, nil
}

func RegistryIndex(records []RegistryRecord) map[string]RegistryRecord {
	index := make(map[string]RegistryRecord, len(records))
	for _, record := range records {
		if record.ModeSCodeHex == "" {
			continue
		}
		index[normalizeICAO24(record.ModeSCodeHex)] = record
	}
	return index
}

func valueAt(row []string, header map[string]int, name string) string {
	idx, ok := header[name]
	if !ok || idx >= len(row) {
		return ""
	}
	return row[idx]
}

func normalizeHeader(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	replacer := strings.NewReplacer(" ", "", "_", "", "-", "", "/", "", "(", "", ")", "", ".", "")
	v = replacer.Replace(v)
	switch v {
	case "nnum", "nnumber":
		return "nnumber"
	case "modecodehex", "modescodehex", "mode_s_code_hex":
		return "modescodehex"
	case "registrant", "ownername", "registrantname":
		return "registrantname"
	case "registranttypecode", "registranttype":
		return "registranttype"
	case "mfr", "make", "manufacturer":
		return "manufacturer"
	case "modelname", "model":
		return "model"
	case "yearmfr", "yearmanufactured", "year":
		return "year"
	case "serialnumber", "serialno":
		return "serialnumber"
	case "aircrafttype":
		return "aircrafttype"
	case "enginetype":
		return "enginetype"
	case "countrycode":
		return "countrycode"
	default:
		return v
	}
}
