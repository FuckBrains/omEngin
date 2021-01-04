
Dim objWMIService, objProcess, colProcess
Dim strComputer, strList

function w2t(data)
	curDate =  Month(Date) & "_" & Day(Date) & "_" & Year(Date) & "_" & Time
	curtime = Replace(CurDate,":","_")
	filename = curtime & ".txt"
	Set fso = CreateObject("Scripting.FileSystemObject")
	pth = fso.GetParentFolderName(WScript.ScriptFullName)
	fullpath = pth & "\" & filename
	Set OutPutFile = fso.OpenTextFile(fullpath ,8 , True)
	OutPutFile.WriteLine(data)
	OutPutFile.Close
	Set fso = Nothing
end function

function usrinp(msg)
	intAnswer = Msgbox(msg, vbYesNo, "user input for info")
	If intAnswer = vbYes Then
		yes_no_chk = 1
	Else
		yes_no_chk = 0
	End If
	usrinp = yes_no_chk
end function

function search_string(lines, srcstr)
	if vartype(lines)=8204 then
		for i=2 to UBound(lines)
			if instr(lines(i),srcstr)<>0 then
				line = line & chr(10) & lines(i)
			end if
		next
	else:
		if instr(lines,srcstr)<>0 then
			line = "exist"
		else
			line = ""
		end if
	end if
	search_string = line
end function

function get_port_info(cmds,prt)
	strline = ""
	Set objShell = WScript.CreateObject("WScript.Shell")
	fcmd = "cmd.exe /c " & cmds
	Set ObjExec = objShell.Exec(fcmd)
	Do
		strFromProc = ObjExec.StdOut.ReadLine()
		colon = instr(strFromProc,":")
		if colon<>0 then
			spac = instr(colon,strFromProc," ")
			mdd = mid(strFromProc,colon+1,spac-colon+1)
			'Wscript.Echo "Cln: " & colon & " _ " & " sp: " & spac & "mdd- [" & trim(mdd) & "]"
			if trim(mdd)=cstr(prt) then
				'Wscript.Echo "VALID----" & trim(mdd)
				strline = strline & chr(10) & strFromProc
			end if
		end if
	Loop While Not ObjExec.Stdout.atEndOfStream
	get_port_info = strline
end function

function get_kill(name_of_prog, action):
	strComputer = "."
	Set objWMIService = GetObject("winmgmts:" _
								& "{impersonationLevel=impersonate}!\\" _
								& strComputer & "\root\cimv2")
	Set colProcess = objWMIService.ExecQuery _
							("Select * from Win32_Process")
	if name_of_prog="" then
		For Each objProcess in colProcess
			strList = strList & vbCr & "Process_Name: " & objProcess.Name & " ,PID: " & objProcess.ProcessId
		Next
		w2t(strList)
	elseif name_of_prog<>"" and action="getinfo" then
		count = 0
		nam = ""
		inp = LCase(name_of_prog)
		For Each objProcess in colProcess
			nm = " " & Lcase(objProcess.Name)
			strList = strList & vbCr & "Process_Name: " & objProcess.Name & " ,PID: " & objProcess.ProcessId
			mat = instr(nm,inp)
			if mat <>0 then
				nam = nam & chr(10) & objProcess.Name
				count = count + 1
			end if
		Next
		if count <> 0 then
			Wscript.Echo "[Not Killing anything], Total process found: " & count & chr(10) & "Following: " & chr(10) & nam
		else
			Wscript.Echo "Process is not running"
		end if
	else
		count = 0
		nam = ""
		inp = LCase(name_of_prog)
		For Each objProcess in colProcess
			nm = " " & Lcase(objProcess.Name)
			strList = strList & vbCr & "Process_Name: " & objProcess.Name & " ,PID: " & objProcess.ProcessId
			mat = instr(nm,inp)
			if mat <>0 then
				nam = nam & chr(10) & objProcess.Name
				objProcess.Terminate()
				count = count + 1
			end if
		Next
		if count <> 0 then
			Wscript.Echo "Total process found: " & count & chr(10) & "Following: " & chr(10) & nam
		else
			Wscript.Echo "Process is not running"
		end if		
	end if
	get_kill = ""
end function

function findpid(y)
	pid = ""
	y1 = replace(y," ","")
	chk1 = instr(y1,"LISTENING")
	chk2 = instrrev(y1,"LISTENING")
	chk1 = instrrev(y1,"TCP")
	if chk1=0 then
		chk1 = instrrev(y1,"UDP")
	end if
	lft = Left(y1, chk1-1)
	cutval = len(lft) - (instr(lft,"LISTENING") + len("LISTENING"))
	pid = Right(lft, cutval+1)
	findpid = pid
end function

function findpd(st, pidval)
	heap = ""
	endat = 0
	str = replace(st," ","")
	chk = instr(str,pidval)
		for i=chk to len(str)
			md = mid(str,i,1)
			if asc(md)>47 and asc(md)<57 then
				endat = endat + 1
			else
				Exit for
			end if
		next
		heap = mid(str,chk,endat)
		findpd = heap
end function

function killby_pid(pidstr)
		strComputer = "."
		Set objWMIS = GetObject("winmgmts:" _
								& "{impersonationLevel=impersonate}!\\" _
								& strComputer & "\root\cimv2")
		Set colP = objWMIS.ExecQuery _
							("Select * from Win32_Process")
		cnt = 0
		For Each objProcess in colP
			pid = objProcess.ProcessId
			if instr(pidstr,pid)<>0 then
				rs = findpd(pidstr,pid)
				msgbox pid & " rs " & rs
				if cint(pid)=cint(rs) then
					objProcess.Terminate()
					cnt = cnt + 1
				end if
			end if
		Next
		msgbox "terminated total process -- " & cnt
		killby_pid = cnt
end function

userhelp = "Enter NAME or PORT of a Program :"
strInput = InputBox(userhelp)

if len(strInput)=0 then
	Msgbox "you did not provide any input, so wrring all process info in text file"
	
elseif IsNumeric(strInput) Then
	uinp1 = usrinp("do u want to kill the program by this port " & strInput)
	if uinp1<>0 then
		Msgbox "trying to kill by port"
		str = "netstat -ano  | findstr :" & strInput
		getdata = get_port_info(str, strInput)
		msgbox getdata
		if len(getdata)>10 then
			xxx = killby_pid(getdata)
		else
			msgbox "port not listening"
		end if
	else
		getdata = get_port_info("netstat -ano")
		w2t(getdata)
	end if
else
	uinp1 = usrinp("do u want to kill the program by this name " & strInput)
	if uinp1<>0 then
		Msgbox "trying to kill by Name"
		x = get_kill(strInput, "")
	else
		if uinp1<>0 then
			x = get_kill(strInput, "getinfo")
			w2t(x)
			msgbox "done from line 168"
		end if
	end if
end if

'netstat -ano | findstr :3306
'Msgbox "you pressed yes, so trying to kill program " & strInput & " [by name]"
'Msgbox "you pressed no, so writting all processing pid data in text file in same dir"