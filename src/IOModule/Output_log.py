import IOModule.Log_print as Log_print

__metaclass__ = type

class Output_log:
    def __init__(self, output = None, log_freq = 1):
        self.myInfo = "Output_log"
        self.output_path = output
        self.sys_info_buf = []
        self.job_buf = []
        self.utilization_buf = []
        self.node_idle_buf = []
        self.log_freq = log_freq
        self.reset_output()
    
    def reset(self, output = None, log_freq = 1):
        if output:
            self.output_path = output
            self.sys_info_buf = []
            self.job_buf = []
            self.utilization_buf = []  # Reset utilization buffer
            self.log_freq = log_freq
            self.reset_output()

    def reset_output(self):   
        self.sys_info = Log_print.Log_print(self.output_path['sys'],0)
        self.sys_info.reset(self.output_path['sys'],0)
        self.sys_info.file_open()
        self.sys_info.file_close()
        self.sys_info.reset(self.output_path['sys'],1)   
        
        self.adapt_info = Log_print.Log_print(self.output_path['adapt'],0)
        self.adapt_info.reset(self.output_path['adapt'],0)
        self.adapt_info.file_open()
        self.adapt_info.file_close()
        self.adapt_info.reset(self.output_path['adapt'],1)
        
        self.job_result = Log_print.Log_print(self.output_path['result'],0)
        self.job_result.reset(self.output_path['result'],0)
        self.job_result.file_open()
        self.job_result.file_close()
        self.job_result.reset(self.output_path['result'],1)

        # Utilization metrics output file
        self.util_result = Log_print.Log_print(self.output_path['util'],0)
        self.util_result.reset(self.output_path['util'],0)
        self.util_result.file_open()
        # Write header
        header = "time;event;total_nodes;idle_nodes;utilization;idle_pct;wait_jobs;wait_procs"
        self.util_result.log_print(header,1)
        self.util_result.file_close()
        self.util_result.reset(self.output_path['util'],1)

        # NEW: Add output file for node idle data
        self.node_idle_log = Log_print.Log_print(self.output_path['node_idle'],0)
        self.node_idle_log.reset(self.output_path['node_idle'],0)
        self.node_idle_log.file_open()
        # Write header
        header = "time;node_id;event;idle_duration"
        self.node_idle_log.log_print(header,1)
        self.node_idle_log.file_close()
        self.node_idle_log.reset(self.output_path['node_idle'],1)
        
        # NEW: Add output file for node state snapshots
        self.node_state_log = Log_print.Log_print(self.output_path['node_state'],0)
        self.node_state_log.reset(self.output_path['node_state'],0)
        self.node_state_log.file_open()
        # Write header
        header = "time;total_nodes;idle_nodes;node_states"
        self.node_state_log.log_print(header,1)
        self.node_state_log.file_close()
        self.node_state_log.reset(self.output_path['node_state'],1)

    def print_sys_info(self, sys_info = None):
        if sys_info != None:
            self.sys_info_buf.append(sys_info)
            # Also capture utilization data for the new output file
            if 'idle_nodes' in sys_info and 'total_nodes' in sys_info:
                self.utilization_buf.append(sys_info)
            
        if (len(self.sys_info_buf) >= self.log_freq) or (sys_info == None):
            sep_sign=";"
            sep_sign_B=" "
            self.sys_info.file_open()
            for sys_info in self.sys_info_buf:
                context = ""
                context += str(int(sys_info['date']))
                context += sep_sign
                context += str(sys_info['event'])
                context += sep_sign
                context += str(sys_info['time'])
                context += sep_sign
                
                context += ('uti'+'='+str(sys_info['uti']))
                context += sep_sign_B
                context += ('waitNum'+'='+str(sys_info['waitNum']))
                context += sep_sign_B
                context += ('waitSize'+'='+str(sys_info['waitSize']))
                
                # Add the new metrics if available
                if 'idle_nodes' in sys_info and sys_info['idle_nodes'] >= 0:
                    context += sep_sign_B
                    context += ('idleNodes'+'='+str(sys_info['idle_nodes']))
                if 'idle_pct' in sys_info:
                    context += sep_sign_B
                    context += ('idlePct'+'='+str(sys_info['idle_pct']))
                
                self.sys_info.log_print(context,1)
            self.sys_info.file_close()
            self.sys_info_buf = []
            
        # Write to the detailed utilization file if we have data
        if (len(self.utilization_buf) >= self.log_freq) or (sys_info == None and len(self.utilization_buf) > 0):
            self.write_utilization_data()
    
    def write_utilization_data(self):
        """Write detailed utilization data to a separate file"""
        if len(self.utilization_buf) == 0:
            return
            
        sep_sign=";"
        self.util_result.file_open()
        for util_info in self.utilization_buf:
            context = ""
            context += str(util_info['time'])
            context += sep_sign
            context += str(util_info['event'])
            context += sep_sign
            
            # Add node counts
            total_nodes = util_info.get('total_nodes', -1)
            idle_nodes = util_info.get('idle_nodes', -1)
            context += str(total_nodes)
            context += sep_sign
            context += str(idle_nodes)
            context += sep_sign
            
            # Add utilization percentages
            context += str(util_info['uti'])
            context += sep_sign
            context += str(util_info.get('idle_pct', 100.0 - (util_info['uti'] * 100.0)))
            context += sep_sign
            
            # Add waiting job information
            context += str(util_info['waitNum'])
            context += sep_sign
            context += str(util_info['waitSize'])
            
            self.util_result.log_print(context,1)
        self.util_result.file_close()
        self.utilization_buf = []
    
    def print_adapt(self, adapt_info):
        sep_sign=";"
        context = ""
        self.adapt_info.file_open()
        self.adapt_info.log_print(context,1)
        self.adapt_info.file_close()

    def print_result(self, job_module, job_index = None):
        if job_index != None:
            self.job_buf.append(job_module.job_info(job_index))
        if (len(self.job_buf) >= self.log_freq) or (job_index == None):
            self.job_result.file_open()
            sep_sign=";"
            for temp_job in self.job_buf:
                context = ""
                context += str(temp_job['id'])
                context += sep_sign
                context += str(temp_job['reqProc'])
                context += sep_sign
                context += str(temp_job['reqProc'])
                context += sep_sign
                context += str(temp_job['reqTime'])
                context += sep_sign
                context += str(temp_job['run'])
                context += sep_sign
                context += str(temp_job['wait'])
                context += sep_sign
                context += str(temp_job['submit'])
                context += sep_sign
                context += str(temp_job['start'])
                context += sep_sign
                context += str(temp_job['end'])
                self.job_result.log_print(context,1)
            self.job_result.file_close()
            self.job_buf = []
            
        # Make sure we flush any pending utilization data when all jobs are processed
        if job_index == None:
            self.write_utilization_data()

    def record_node_idle(self, time, node_id, event, idle_duration):
        """Record a node idle period event"""
        self.node_idle_buf.append({
            'time': time,
            'node_id': node_id,
            'event': event,  # 'START' or 'END'
            'duration': idle_duration
        })
        
        if len(self.node_idle_buf) >= self.log_freq:
            self.write_node_idle_data()

    # Add a new method for recording node state snapshots:
    def record_node_states(self, time, node_states):
        """Record a snapshot of all node states in a compact format"""
        idle_count = sum(1 for node in node_states if node['is_idle'])
        total_count = len(node_states)
        
        # Instead of storing the entire node_states list, just store the essential information
        # Create a compact representation of node states
        # Format: List of idle node IDs and their idle times
        idle_nodes = []
        for node in node_states:
            if node['is_idle']:
                idle_nodes.append((node['node_id'], node['idle_time']))
        
        self.node_state_log.file_open()
        # Write only the time, counts, and the compact representation
        context = f"{time};{total_count};{idle_count};{idle_nodes}"
        self.node_state_log.log_print(context, 1)
        self.node_state_log.file_close()

    # Add a method to write the node idle data:
    def write_node_idle_data(self):
        """Write node idle period data to file"""
        if len(self.node_idle_buf) == 0:
            return
            
        self.node_idle_log.file_open()
        sep_sign = ";"
        for event in self.node_idle_buf:
            context = ""
            context += str(event['time'])
            context += sep_sign
            context += str(event['node_id'])
            context += sep_sign
            context += str(event['event'])
            context += sep_sign
            context += str(event['duration'])
            
            self.node_idle_log.log_print(context, 1)
        self.node_idle_log.file_close()
        self.node_idle_buf = []