    strComputer = "."
    Set objWMIS = GetObject("winmgmts:" _
								& "{impersonationLevel=impersonate}!\\" _
								& strComputer & "\root\cimv2")
	Set colP = objWMIS.ExecQuery ("Select * from Win32_Process")