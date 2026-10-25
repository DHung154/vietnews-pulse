Option Explicit

' WScript has no console; Run style 0 hides PowerShell from process creation.
If WScript.Arguments.Count <> 1 Then WScript.Quit 2

Dim shell, files, projectRoot, python, powershell, command
Set shell = CreateObject("WScript.Shell")
Set files = CreateObject("Scripting.FileSystemObject")
python = WScript.Arguments(0)
If Not files.FileExists(python) Then WScript.Quit 2
projectRoot = files.GetParentFolderName(files.GetParentFolderName(WScript.ScriptFullName))
shell.CurrentDirectory = projectRoot
powershell = shell.ExpandEnvironmentStrings("%SystemRoot%") & "\System32\WindowsPowerShell\v1.0\powershell.exe"
command = Quoted(powershell) & " -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File " & _
    Quoted(projectRoot & "\scripts\run_collector.ps1") & " -PythonExecutable " & Quoted(python)
WScript.Quit shell.Run(command, 0, True)

Function Quoted(value)
    Quoted = Chr(34) & value & Chr(34)
End Function
