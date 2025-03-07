import os
import csv
from datetime import datetime

__metaclass__ = type

class IdleTracker:
    def __init__(self, output_path, debug = None):
        self.myInfo = "Idle Tracker"
        self.debug = debug
        self.output_path = output_path
        
        # Time series of idle node counts
        self.idle_timeseries = []  # List of (timestamp, idle_count) tuples
        
        # Track idle events for each node
        self.node_idle_events = {}  # Dict of node_id -> list of (start_time, end_time) tuples
        self.node_states = {}       # Dict of node_id -> current state (1 for busy, 0 for idle)
        self.idle_events = []       # List of (node_id, start_time, end_time, duration) tuples
        
        # Make sure output directory exists
        self.ensure_output_path()
        
        # Logging initial state
        if self.debug:
            self.debug.line(4," ")
            self.debug.line(4,"#")
            self.debug.debug("# "+self.myInfo,1)
            self.debug.line(4,"#")
    
    def ensure_output_path(self):
        """Ensure the output directory exists"""
        if not os.path.exists(os.path.dirname(self.output_path)):
            try:
                os.makedirs(os.path.dirname(self.output_path))
            except OSError as exc:
                if exc.errno != os.errno.EEXIST:
                    raise
    
    def initialize_nodes(self, node_list):
        """Initialize the tracker with a list of nodes"""
        for node in node_list:
            node_id = node['id']
            self.node_states[node_id] = 0  # Initially all nodes are idle
            self.node_idle_events[node_id] = []
            
            # Mark the start of an idle period for this node
            self.node_idle_events[node_id].append((0, None))  # End time will be set when the node becomes busy
        
        # Record initial idle count
        self.record_idle_count(0)
        
        if self.debug:
            self.debug.debug("Initialized idle tracker with " + str(len(node_list)) + " nodes", 3)
    
    def node_state_change(self, node_id, state, time):
        """Record a node state change (1 for busy, 0 for idle)"""
        if node_id not in self.node_states:
            # Node not tracked yet
            self.node_states[node_id] = state
            self.node_idle_events[node_id] = []
            
            if state == 0:  # Node starts idle
                self.node_idle_events[node_id].append((time, None))
            return
        
        prev_state = self.node_states[node_id]
        
        if prev_state == state:
            # No state change
            return
        
        if state == 0:  # Node became idle
            # Start a new idle period
            self.node_idle_events[node_id].append((time, None))
        else:  # Node became busy
            # Complete the last idle period if there is one
            if self.node_idle_events[node_id] and self.node_idle_events[node_id][-1][1] is None:
                start_time = self.node_idle_events[node_id][-1][0]
                self.node_idle_events[node_id][-1] = (start_time, time)
                
                # Record the completed idle event
                duration = time - start_time
                self.idle_events.append((node_id, start_time, time, duration))
        
        # Update node state
        self.node_states[node_id] = state
        
        if self.debug:
            self.debug.debug("Node " + str(node_id) + " changed state to " + str(state) + " at time " + str(time), 4)
    
    def record_idle_count(self, time):
        """Record the current count of idle nodes"""
        idle_count = sum(1 for state in self.node_states.values() if state == 0)
        self.idle_timeseries.append((time, idle_count))
        
        if self.debug:
            self.debug.debug("Recorded idle count: " + str(idle_count) + " at time " + str(time), 4)
    
    def finish_simulation(self, end_time):
        """Complete any open idle periods at the end of simulation"""
        for node_id, events in self.node_idle_events.items():
            if events and events[-1][1] is None:
                start_time = events[-1][0]
                events[-1] = (start_time, end_time)
                
                # Record the completed idle event
                duration = end_time - start_time
                self.idle_events.append((node_id, start_time, end_time, duration))
        
        # Final idle count
        self.record_idle_count(end_time)
        
        if self.debug:
            self.debug.debug("Finished simulation, completed all idle events", 3)
    
    def write_results(self):
        """Write the tracking results to output files"""
        # Write idle timeseries
        timeseries_file = self.output_path + "_idle_timeseries.csv"
        with open(timeseries_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'idle_node_count'])
            for timestamp, idle_count in self.idle_timeseries:
                writer.writerow([timestamp, idle_count])
        
        # Write idle events
        events_file = self.output_path + "_idle_events.csv"
        with open(events_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['node_id', 'start_time', 'end_time', 'duration'])
            for node_id, start_time, end_time, duration in self.idle_events:
                writer.writerow([node_id, start_time, end_time, duration])
        
        if self.debug:
            self.debug.debug("Wrote results to " + timeseries_file + " and " + events_file, 2)
            self.debug.debug("Total idle events recorded: " + str(len(self.idle_events)), 2)
            self.debug.debug("Timeseries points recorded: " + str(len(self.idle_timeseries)), 2)