import os
import logging

from pathlib import Path

import jinja2
import jinja2.nodes
import jinja2.ext

logger = logging.getLogger(__name__)


class SkipThisTemplate(Exception):
    "May be raised from templates when the whole file should be skipped."


class SkipTemplateTagExtension(jinja2.ext.Extension):

    tags = {'skip_file_when', 'skip_file_unless'}

    def parse(self, parser):
        token = next(parser.stream)
        lineno = token.lineno
        cond = parser.parse_expression()
        return jinja2.nodes.CallBlock(
            self.call_method(token.value, [cond], lineno=lineno),
            [], [], [], lineno=lineno,
        )

    def skip_file_when(self, cond, caller):
        if cond:
            raise SkipThisTemplate()
        return ""

    def skip_file_unless(self, cond, caller):
        return self.skip_file_when(not cond, caller)


def render_templates(
    dest: os.PathLike,
    templates: jinja2.BaseLoader,
    variables: dict,
):
    dest = Path(dest)
    logger.info("render templates into %s", dest)

    env = jinja2.Environment(
        loader=templates,
        extensions=[SkipTemplateTagExtension],
    )
    env.filters['repr'] = repr
    env.filters['str'] = str

    logger.debug("jinja2 globals: %r", variables)
    env.globals = variables

    for template_name in env.list_templates():
        logger.debug("render template %r", template_name)

        name_expr = template_name
        if "." in template_name:
            base, ext = template_name.rsplit(".", 1)
            if ext in {'jinja', 'j2'}:
                name_expr = base

        name_template = env.from_string(name_expr)
        body_template = env.get_template(template_name)

        try:
            file_name = name_template.render()
            body = body_template.render()
        except SkipThisTemplate:
            logger.debug("skip template %r", template_name)
            continue

        dst_path = dest / file_name

        if not dst_path.parent.exists():
            dst_path.parent.mkdir(parents=True)

        try:
            dst_path.unlink()
        except FileNotFoundError:
            pass

        logger.info("create file %s", dst_path)
        dst_path.write_text(body)


def render_builtin_templates(dest: os.PathLike, template_dir: str, variables: dict):
    loader = jinja2.PackageLoader('bigflow.scaffold', f"templates/{template_dir}")
    logger.debug("load jinja2 templates from %s", loader)
    render_templates(dest, loader, variables)
