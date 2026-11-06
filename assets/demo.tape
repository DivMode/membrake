# Records assets/demo.gif with VHS (https://github.com/charmbracelet/vhs):
#   PATH="<dir with membrake>:$PATH" vhs assets/demo.tape
Output assets/demo.gif
Set Shell "bash"
Set FontSize 15
Set Width 1500
Set Height 760
Set Padding 24
Set Theme "Catppuccin Mocha"
Set TypingSpeed 45ms
Hide
Type "export PS1='$ '; clear" Enter
Show
Type "membrake status" Sleep 300ms Enter
Sleep 3.5s
Type "clear" Enter
Type "# replay a simulated crunch: four agent type-checks and a CI shard fill memory" Enter
Sleep 600ms
Type "membrake replay assets/simulated-crunch.tsv" Sleep 300ms Enter
Sleep 9s
