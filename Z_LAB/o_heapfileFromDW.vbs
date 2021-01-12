Set FSO = CreateObject("Scripting.FileSystemObject")
Set objShell = WScript.CreateObject("WScript.Shell")


function heap(fromdir)
	strx = ""
	Set objFolder = FSO.GetFolder(fromdir)
	Set colFiles = objFolder.Files
	For Each objFile in colFiles
		if instr(objFile.Name, ".ipynb")=0 then
			FileName = fromdir & objFile.Name
			Set fp1 = FSO.OpenTextFile(FileName,1)
			stt = fp1.ReadAll()
			strx = strx & chr(10) & chr(10) & "###" & FileName & "###" & chr(10) & stt & chr(10) & "$$$$$$$$$" & chr(10)
			if err = True then
				err.Clear
			end if
			fp1.Close
		end if
	next
	heap = strx
end Function


sub WFile(strx,tofile)
	Set fpw = FSO.OpenTextFile(tofile,2,true)
	fpw.Write strx
	fpw.close()
end sub

thispt = FSO.GetParentFolderName(WScript.ScriptFullName)
fromdir = thispt & "\DW\"
totxt = thispt & "\" & Month(now) & Day(now) & Year(now) & "-" & Hour(now) & Minute(now) & ".txt"
str = heap(fromdir)
WFile str,totxt
WScript.Echo "Done"
