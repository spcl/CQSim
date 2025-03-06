import IOModule.Log_print as Log_print

__metaclass__ = type

class Output_log:
    def __init__(self, output = None, log_freq = 1):
        self.myInfo = "Output_log"
        self.output_path = output
        self.sys_info_buf = []
        self.job_buf = []
        self.utilization_buf = []  # New buffer for detailed utilization data
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

        # New output file for detailed utilization metrics
        self.util_result = Log_print.Log_print(self.output_path['util'],0)
        self.util_result.reset(self.output_path['util'],0)
        self.util_result.file_open()
        # Write header
        header = "time;event;total_nodes;idle_nodes;utilization;idle_pct;wait_jobs;wait_procs"
        self.util_result.log_print(header,1)
        self.util_result.file_close()
        self.util_result.reset(self.output_path['util'],1)

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