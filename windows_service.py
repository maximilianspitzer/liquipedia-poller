import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys
import os
from service import LiquipediaPollerService

class LiquipediaPollerWindowsService(win32serviceutil.ServiceFramework):
    _svc_name_ = "LiquipediaPoller"
    _svc_display_name_ = "Liquipedia Esports Stats Poller"
    _svc_description_ = "Polls Liquipedia for Brawl Stars esports statistics and maintains a database"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.service = LiquipediaPollerService()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        self.service.stop()

    def SvcDoRun(self):
        try:
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PID_INFO,
                ("Service starting", "")
            )
            self.service.run()
        except Exception as e:
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_ERROR_TYPE,
                servicemanager.PID_INFO,
                (str(e), "")
            )
            raise

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(LiquipediaPollerWindowsService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(LiquipediaPollerWindowsService)