import pandas as pd
from django.http import HttpResponse
try:
    from io import BytesIO as IO # for modern python
except ImportError:
    from StringIO import StringIO as IO # for legacy python

def download_excel(request):
    if "selectdate" in request.POST:
        if "selectaccount" in request.POST:
            selected_date = request.POST["selectdate"]
            selected_acc = request.POST["selectaccount"]
        if selected_date==selected_date:
            if selected_acc==selected_acc:
                convert=datetime.datetime.strptime(selected_date, "%Y-%m-%d").toordinal()
                engine=create_engine('mssql+pymssql://username:password@servername /db')

               
                metadata=MetaData(connection)
                fund=Table('gltrxdet',metadata,autoload=True,autoload_with=engine)
                rate=Table('gltrx_all',metadata,autoload=True,autoload_with=engine)
                stmt=select([fund.columns.account_code,fund.columns.description,fund.columns.nat_balance,rate.columns.date_applied,fund.columns.journal_ctrl_num,rate.columns.journal_ctrl_num])
                stmt=stmt.where(and_(rate.columns.journal_ctrl_num==fund.columns.journal_ctrl_num,fund.columns.account_code==selected_acc,rate.columns.date_applied==convert))
                results=connection.execute(stmt)
                
                sio = StringIO()
                df = pd.DataFrame(data=list(results), columns=results.keys())

                ####dowload excel file##########
                excel_file = IO()
                xlwriter = pd.ExcelWriter(excel_file, engine='xlsxwriter')
                df.to_excel(xlwriter, 'sheetname')
                xlwriter.save()
                xlwriter.close()
                excel_file.seek(0)

                response = HttpResponse(excel_file.read(), content_type='application/ms-excel vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                # set the file name in the Content-Disposition header
                response['Content-Disposition'] = 'attachment; filename=myfile.xls'
                return response