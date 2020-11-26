This folder contains templates for scaffolding.
See bigflow/scaffold/templating.py for more details.

Top-level folder name is a template name.
All files inside the folder are considered as a Jinja2 templates,
suffixes ".j2" and ".jinja" are stripped (for better editor/ide support).
It is allowed to use jinja2 expressions inside filenames.

Please add '.jinja' suffix to all Python files (even when they are not templates)
to prevent setuptools from compiling them into *.pyc files.

Empty folders are not supported (like `git`), but you can create an empty `.gitkeep` to workaround this.

There are two non-default macroses: `skip_file_unless` and `skip_file_when`.
You can use them for conditional rendering when presence of file depends on some variables.