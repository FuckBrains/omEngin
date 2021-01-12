
Set FSO = CreateObject("Scripting.FileSystemObject")
varPathCurrent = FSO.GetParentFolderName(WScript.ScriptFullName)
Set objShell = WScript.CreateObject("WScript.Shell")

function fny(filepth, topth)
	If FSO.FileExists(filepth) Then
		Set of1 = FSO.GetFile(filepth)
		sName = of1.Name
		fl=left(sName, instrrev(sName,"."))
		ext=right(sName, len(sName)-instrrev(sName,"."))
		nf = topth & sName
		if FSO.FileExists(nf) Then
			Set of2 = FSO.GetFile(nf)
			if of1.size<>of2.size Then
				LastMod = of1.DateLastModified
				fmt = Month(LastMod) & Day(LastMod) & Year(LastMod) & "-" & Hour(LastMod) & Minute(LastMod)
				nwf = topth & fmt & "-XAQ-" & fl & ext
				FSO.CopyFile filepth, nwf
			Else
				nwf = "same size"
			end If
		Else
			FSO.CopyFile filepth, nf
		end if
	end If
	fny = nwf
end Function





function rdwt(rdf,append_to)
	On Error Resume Next
	Set objFolder = FSO.GetFolder(rdf)
	Set colFiles = objFolder.Files
	str = "1"
	cnt = 0
	For Each objFile in colFiles
		if instr(objfile,".ipynb")=0 then
			cnt = cnt + 1
			wscript.Echo cnt
			FileName = rdf & objFile.Name
			Set fp = FSO.OpenTextFile(FileName,1)
			stt = fp.ReadAll()
			strx = strx & chr(10) & chr(10) & "###" & FileName & "###" & chr(10) & stt & chr(10) & "$$$$$$$$$" & chr(10)
			if err = True then
				err.Clear
			end if
			fp.Close
		end if
	next
	Set fpw = FSO.OpenTextFile(append_to,2)
	WScript.echo strx
	fpw.Write strx
	fpw.close()
end function

function cpy(txfile,jy,py)
	Set fp = FSO.OpenTextFile(txfile)
	Do Until fp.AtEndOfStream
	strLine = fp.Readline
	ext=right(strLine, len(strLine)-instrrev(strLine,"."))
		if len(strLine)>2 Then
			if instr(strLine,".")>1 and ext<>"ipynb" Then
				xx = fny(strLine,py)
			elseif instr(strLine,".")>1 and ext="ipynb" Then
				xx = fny(strLine,jy)
			end if
		end if
	Loop
end function


'wscript.Echo rdwt(x,ALpy)

function dirloop(dr)
	Set objFolder = FSO.GetFolder(dr)
	Set colFiles = objFolder.Files
	For Each objFile in colFiles
		FileName = dr & objFile.Name
		if instr(objFile.Name,".")>1 then
			objShell.Run("cmd /k jupyter nbconvert --to script " & FileName)
			WScript.Sleep 1000
		end if
	next
end function

txfile = varPathCurrent & "\B.txt"
ALjy = varPathCurrent & "\aljy.txt"
ALpy = varPathCurrent & "\alpy.txt"
jy = varPathCurrent & "\Z_ALL_FILE\Jy1\"
py = varPathCurrent & "\Z_ALL_FILE\Py1\"
'x1=cpy(txfile,jy,py)
x2 = rdwt(py, ALpy)
x2 = rdwt(jy, ALjy)

