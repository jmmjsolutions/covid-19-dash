import os

html_template = '''<!DOCTYPE html>
                    <html>
                        <head>
                            {%metas%}
                            <title>{%title%}</title>
                            {%favicon%}
                            {%css%}
                        </head>
                        <body>
                            $nav$
                            {%app_entry%}
                            <footer>
                                {%config%}
                                {%scripts%}
                                {%renderer%}
                            </footer>
                        </body>
                    </html>'''

path = os.path.dirname(__file__)
nav_tmpl = os.path.join(path, '../templates/nav.html')
with open(nav_tmpl) as f:
    nav_template = ''.join(f.readlines())
html_layout = html_template.replace('$nav$', nav_template)