DetectHiddenWindows, On
DetectHiddenText, On

SetWorkingDir %A_ScriptDir%

for process in ComObjGet("winmgmts:").ExecQuery("Select * from Win32_Process  where CommandLine like '%python.exe%' ")
		process, close,  % process.ProcessId
for process in ComObjGet("winmgmts:").ExecQuery("Select * from Win32_Process  where CommandLine like '%cmd.exe%' ")
		process, close,  % process.ProcessId
sleep, 5000
Run, E:\OmPEnv\venv\Scripts\python.exe E:\TBOT\main.py
send, #d
sleep, 2000
;run, cmd FOR /f %%G IN ('query session /sm') DO tsdiscon %%G
ExitApp