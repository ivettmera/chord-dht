package metrics

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Metrics represents the metrics collector for a Chord node
type Metrics struct {
	mu             sync.RWMutex
	nodeID         string
	outputDir      string
	experimentID   string
	
	// Metrics data
	timestamp      time.Time
	nodeCount      int
	messageCount   int64
	lookupCount    int64
	lookupLatency  []time.Duration
	
	// CSV writer
	csvFile   *os.File
	csvWriter *csv.Writer
	
	// Background writer
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewMetrics creates a new metrics collector
func NewMetrics(nodeID, outputDir, experimentID string) (*Metrics, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}
	
	filename := fmt.Sprintf("node_%s_%s.csv", nodeID[:8], experimentID)
	filepath := filepath.Join(outputDir, filename)
	
	file, err := os.Create(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}
	
	writer := csv.NewWriter(file)
	
	// Write CSV header
	header := []string{"timestamp", "nodes", "messages", "lookups", "avg_lookup_ms"}
	if err := writer.Write(header); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}
	writer.Flush()
	
	m := &Metrics{
		nodeID:       nodeID,
		outputDir:    outputDir,
		experimentID: experimentID,
		timestamp:    time.Now(),
		csvFile:      file,
		csvWriter:    writer,
		stopChan:     make(chan struct{}),
	}
	
	// Start background metrics writer
	m.startPeriodicWriter()
	
	log.Printf("Metrics initialized for node %s, output: %s", nodeID[:8], filepath)
	return m, nil
}

// RecordLookup records a lookup operation with its latency
func (m *Metrics) RecordLookup(latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.lookupCount++
	m.lookupLatency = append(m.lookupLatency, latency)
}

// RecordMessage records a message sent or received
func (m *Metrics) RecordMessage() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.messageCount++
}

// UpdateNodeCount updates the number of nodes in the ring
func (m *Metrics) UpdateNodeCount(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.nodeCount = count
}

// WriteSnapshot writes current metrics to CSV
func (m *Metrics) WriteSnapshot() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	avgLatency := float64(0)
	if len(m.lookupLatency) > 0 {
		totalLatency := time.Duration(0)
		for _, lat := range m.lookupLatency {
			totalLatency += lat
		}
		avgLatency = float64(totalLatency.Nanoseconds()) / float64(len(m.lookupLatency)) / 1e6 // Convert to milliseconds
	}
	
	record := []string{
		fmt.Sprintf("%d", time.Now().Unix()),
		fmt.Sprintf("%d", m.nodeCount),
		fmt.Sprintf("%d", m.messageCount),
		fmt.Sprintf("%d", m.lookupCount),
		fmt.Sprintf("%.2f", avgLatency),
	}
	
	if err := m.csvWriter.Write(record); err != nil {
		return fmt.Errorf("failed to write CSV record: %w", err)
	}
	
	m.csvWriter.Flush()
	
	// Reset lookup latency for next snapshot but keep counters
	m.lookupLatency = m.lookupLatency[:0]
	
	return nil
}

// startPeriodicWriter starts a goroutine that writes metrics every 30 seconds
func (m *Metrics) startPeriodicWriter() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-m.stopChan:
				return
			case <-ticker.C:
				if err := m.WriteSnapshot(); err != nil {
					log.Printf("Failed to write metrics snapshot: %v", err)
				}
			}
		}
	}()
}

// Close closes the CSV file and stops background writer
func (m *Metrics) Close() error {
	close(m.stopChan)
	m.wg.Wait()
	
	// Write final snapshot
	if err := m.WriteSnapshot(); err != nil {
		log.Printf("Failed to write final metrics snapshot: %v", err)
	}
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.csvWriter != nil {
		m.csvWriter.Flush()
	}
	
	if m.csvFile != nil {
		return m.csvFile.Close()
	}
	
	return nil
}

// GetCurrentStats returns current statistics
func (m *Metrics) GetCurrentStats() (int, int64, int64, float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	avgLatency := float64(0)
	if len(m.lookupLatency) > 0 {
		totalLatency := time.Duration(0)
		for _, lat := range m.lookupLatency {
			totalLatency += lat
		}
		avgLatency = float64(totalLatency.Nanoseconds()) / float64(len(m.lookupLatency)) / 1e6
	}
	
	return m.nodeCount, m.messageCount, m.lookupCount, avgLatency
}

// GlobalMetrics combines metrics from multiple nodes
type GlobalMetrics struct {
	OutputDir    string
	ExperimentID string
}

// NewGlobalMetrics creates a new global metrics collector
func NewGlobalMetrics(outputDir, experimentID string) *GlobalMetrics {
	return &GlobalMetrics{
		OutputDir:    outputDir,
		ExperimentID: experimentID,
	}
}

// CombineNodeMetrics combines metrics from all node CSV files into a global CSV
func (gm *GlobalMetrics) CombineNodeMetrics() error {
	if err := os.MkdirAll(gm.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}
	
	filename := fmt.Sprintf("global_%s.csv", gm.ExperimentID)
	filepath := filepath.Join(gm.OutputDir, filename)
	
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create global CSV file: %w", err)
	}
	defer file.Close()
	
	writer := csv.NewWriter(file)
	defer writer.Flush()
	
	// Write CSV header
	header := []string{"timestamp", "total_nodes", "total_messages", "total_lookups", "avg_lookup_ms"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	
	// For now, just create the file structure
	// In a real implementation, you would read all node CSV files and combine them
	log.Printf("Global metrics file created: %s", filepath)
	
	return nil
}