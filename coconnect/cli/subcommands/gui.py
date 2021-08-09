import click
from .map import run

@click.command(help="run as a Graphical User Interface (GUI)")
@click.pass_context
def gui(ctx):
    import PySimpleGUI as sg

    coconnect_theme = {'BACKGROUND': '#475da7',
                       'TEXT': '#FFFFFF',
                       'INPUT': '#c4c6e2',
                       'TEXT_INPUT': '#000000',
                       'SCROLL': '#c7e78b',
                       'BUTTON': ('white', '#3DB28C'),
                       'PROGRESS': ('#01826B', '#D0D0D0'),
                       'BORDER': 1,
                       'SLIDER_DEPTH': 0,
                       'PROGRESS_DEPTH': 0}
    
    # Add your dictionary to the PySimpleGUI themes
    sg.theme_add_new('coconnect', coconnect_theme)
    sg.theme('coconnect')
    
    layout = [
        [sg.T('Select the rules json:')],
        [sg.Input(key='_RULES_'), sg.FilesBrowse()],
        [sg.T('Select the input CSVs:')],
        [sg.Input(key='_INPUTS_'), sg.FilesBrowse()],
        [sg.OK('Run'), sg.Cancel()]
    ]

    font = ("Roboto", 15)
    
    window = sg.Window('COCONNECT', layout, font=font)
    while True:
        event, values = window.Read()
        
        if event == 'Cancel' or event == None:
            break
    
        rules = values['_RULES_']
        if rules == '':
            sg.Popup(f'Error: please select a rules file')
            continue
        elif len(rules.split(';'))>1:
            sg.Popup(f'Error: only select one file for the rules!')
            continue

        inputs = values['_INPUTS_']
        if inputs == '':
            sg.Popup(f'Error: please select at least one file or directory for the inputs')
            continue
        
        inputs = inputs.split(';')
        ctx.invoke(run,rules=rules,inputs=inputs)
        sg.Popup("Done!")
        break
        
    window.close()
