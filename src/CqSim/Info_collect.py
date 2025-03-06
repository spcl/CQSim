from datetime import datetime
import time
__metaclass__ = type

class Info_collect:
    def __init__(self, alg_module = None, debug = None):
        self.myInfo = "Info Collect"
        self.alg_module = alg_module
        self.debug = debug
        
        self.debug.line(4," ")
        self.debug.line(4,"#")
        self.debug.debug("# "+self.myInfo,1)
        self.debug.line(4,"#")
        
        
    def reset(self, alg_module = None, debug = None):
        self.debug.debug("* "+self.myInfo+" -- reset",5)
        if alg_module:
            self.alg_module = alg_module
        if debug:
            self.debug = debug
        
    def info_collect(self, time, event, uti, waitNum = -1, waitSize = -1, inter = -1.0, extend = None, idle_nodes = -1, total_nodes = -1):
        self.debug.debug("* "+self.myInfo+" -- info_collect",5)
        event_date = time
        
        # Calculate additional resource utilization metrics
        if idle_nodes == -1 and total_nodes == -1:
            # Use provided utilization if specific node counts not available
            idle_resources_pct = 100.0 - (uti * 100.0)
        else:
            idle_resources_pct = (idle_nodes * 100.0) / total_nodes
        
        temp_info = {
            'date': event_date, 
            'time': time, 
            'event': event, 
            'uti': uti, 
            'waitNum': waitNum, 
            'waitSize': waitSize, 
            'inter': inter, 
            'extend': extend,
            # New metrics for resource underutilization analysis
            'idle_nodes': idle_nodes,
            'total_nodes': total_nodes,
            'idle_pct': idle_resources_pct
        }
        
        self.debug.debug("   "+str(temp_info),4) 
        return temp_info