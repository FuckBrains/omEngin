
Set FSO = CreateObject("Scripting.FileSystemObject")
varPathCurrent = FSO.GetParentFolderName(WScript.ScriptFullName)
Set objShell = WScript.CreateObject("WScript.Shell")

function fny(filepth, topth)
	If FSO.FileExists(filepth) Then
		Set of1 = FSO.GetFile(filepth)
		sName = objFile.Name
		fl=left(sName, instrrev(sName,"."))
		ext=right(sName, len(sName)-instrrev(sName,"."))
		nf = topth & sName
		if FSO.FileExists(nf) Then
			Set of2 = FSO.GetFile(nf)
			if of1.size<>of2.size Then
				LastMod = of1.DateLastModified
				fmt = Month(LastMod) & Day(LastMod) & Year(LastMod) & "-" & Hour(LastMod) & Minute(LastMod)
				nwf = topth & fmt & fl & ext
				Wscript.echo nwf
			Else
				Wscript.echo "same size"
			end If
		end If
	end If
end Function



function fxn(filepth, topth)
	nwfile=""
	If FSO.FileExists(filepth) Then
		Set objFile = FSO.GetFile(filepth)
		sName = objFile.Name
		LastMod = objFile.DateLastModified
		sz = objFile.Size
		fmt = Month(LastMod) & Day(LastMod) & Year(LastMod) & "-" & Hour(LastMod) & Minute(LastMod)
		tpth = topth & sName
		fl=left(sName, instrrev(sName,"."))
		ext=right(sName, len(sName)-instrrev(sName,"."))
		'Wscript.echo fl & "##" & ext
		if ext="py" or ext=".py" then
			nwfile = topth & fl & ext
			If FSO.FileExists(nwfile) Then
				Set objF = FSO.GetFile(nwfile)
				if ibjF.Size <> sz then
				nwfile = topth & fl & "_" & fmt & "txt"
			end if
		elseif ext="ipynb" then
			nwfile = topth & sName
			If FSO.FileExists(nwfile) Then
				nwfile = topth & fl & "_" & fmt & ext
			end if
		else
			Wscript.Echo "NA"
			fxn = "NA"
			exit function
		end if
		FSO.CopyFile filepth, nwfile, True
	else
		Wscript.Echo "not_exist"
	end if
	fxn = nwfile
end function

function rdwt(rdf,append_to)
	rdf2 = replace(rdf,"ipynb","txt")
	If FSO.FileExists(rdf2) Then
		Set fp = FSO.OpenTextFile(rdf2,1,False,-1)
		str = chr(10) & "###" & rdf & "###" & fp.ReadAll() & chr(10) & "$$$$$$$$$" & chr(10)
		fp.Close
		WScript.echo str
		wrt = FSO.OpenTextFile(append_to,8,False,-1)
		wrt.WriteLine(str)
		wrt.clsoe()
		rdwt = "OK"
	Else
		Wscript.Echo "Not Write: " & rdf
	end if
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
			'WScript.echo "cmd /c jupyter nbconvert --to script " & FileName
			objShell.Run("cmd /c jupyter nbconvert --to script " & FileName)
			WScript.Sleep 1000
		end if
	next
end function

txfile = varPathCurrent & "\B.txt"
ALjy = varPathCurrent & "\aljy.txt"
ALpy = varPathCurrent & "\alpy.txt"
jy = varPathCurrent & "\Z_ALL_FILE\Jy1\"
py = varPathCurrent & "\Z_ALL_FILE\Py1\"
x1=cpy(txfile,jy,py)
'x2 = dirloop(jy)

