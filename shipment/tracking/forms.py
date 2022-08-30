from django import forms
from datetimepicker.widgets import DateTimePicker

class DateForm(forms.Form):
    date = forms.DateTimeField(
        input_formats=['%d/%m/%Y %H:%M'],
        widget=forms.DateTimeInput(attrs={
            'class': 'form-control datetimepicker-input',
            'data-target': '#datetimepicker1'
        })
    )

class SampleForm(forms.Form):
    datetime = forms.DateTimeField(
    widget=DateTimePicker(),
    )


