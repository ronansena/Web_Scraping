from ast import Break
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import mysql.connector
import datetime

class TabelaCeasa:
	def __init__(self,descricaoAlimento,preco): 
		self.descricaoAlimento = descricaoAlimento	
		self.preco = preco

class Produto:
	def __init__(self,idproduto): 
		self.idproduto = idproduto				

today = datetime.datetime.now()
time = str(today)[10:19]
dataHora = str(today)[0:10]
#print("Today's date:", dataHora)
#print("Today's time:", time)

try:
    req = Request('http://minas1.ceasa.mg.gov.br/ceasainternet/cst_precosmaiscomumMG/cst_precosmaiscomumMG.php', headers={'User-Agent': 'Mozilla/5.0'})    
    html = urlopen(req)
except HTTPError as e:
    print(e)
else:
    bs = BeautifulSoup(html.read(), 'html.parser')

    nomeProdutoList = bs.findAll("td",{"class":["scGridFieldOddFont css_produto_grid_line","scGridFieldEvenFont css_produto_grid_line"]})                                                        
    precoBhList = bs.findAll("td",{"class":["scGridFieldOddFont css_precosbh_grid_line","scGridFieldEvenFont css_precosbh_grid_line"]})    
    dataCotacao = bs.findAll("td",{"class":"css_rodape"})    
    unidadeProdutoList = bs.findAll("td",{"class":["scGridFieldOddFont css_unidade_grid_line","scGridFieldEvenFont css_unidade_grid_line"]})                                                        
    
    dataCotacaoArray = [] 
    dataCotacaoArray = dataCotacao[6].get_text()[38:49].split("/")
    dia = dataCotacaoArray[0].strip() 
    mes = dataCotacaoArray[1].strip()             
    ano = dataCotacaoArray[2].strip()                   
    dataHoraCotacao = ano+'-'+mes+'-'+dia

    print("Data Última Cotação Ceasa MG:"+dataHoraCotacao)
    produtoListArray = [] 

    for idx, name in enumerate(nomeProdutoList):
        nomeProduto = name.get_text().rstrip() + " "+ unidadeProdutoList[idx].get_text().rstrip()
        preco =precoBhList[idx].get_text().strip()
        if(preco == "------"):
            preco = 0
        produtoList = TabelaCeasa(nomeProduto,preco)            
        produtoListArray.append(produtoList)	
        
    def gravacotacao(db1):
        for row in produtoListArray:            
            sqlConsultaProduto = 'select idproduto from produto  where nomeProduto="'+row.descricaoAlimento+'" and idcategoriaDespesa in (28) and idsubcategoriaDespesa=54 '			                      
            cursorConsultaProduto = db1.cursor(dictionary=True)
            cursorConsultaProduto.execute(sqlConsultaProduto)     
            records = cursorConsultaProduto.fetchall()   
            recordsConsultaProduto = records
            if(len(recordsConsultaProduto)==0):
                sqlConsultaUltimoIdProduto = 'select idproduto from produto order by idproduto desc limit 1'	
                cursorConsultaUltimoIdProduto = db1.cursor(dictionary=True)
                cursorConsultaUltimoIdProduto.execute(sqlConsultaUltimoIdProduto)     
                recordsConsultaUltimoIdProduto = cursorConsultaUltimoIdProduto.fetchall()	
                ultimoIdProduto = recordsConsultaUltimoIdProduto[0]['idproduto'] + 1
                sqlInsertProduto = 'insert into produto (idproduto,nomeProduto,idsubcategoriaDespesa,idcategoriaDespesa) values ('+str(ultimoIdProduto)+','+'"'+str(row.descricaoAlimento)+'"'+',54,28)'			
                cursorInsertProduto = db1.cursor(dictionary=True)
                #print(sqlInsertProduto)
                cursorInsertProduto.execute(sqlInsertProduto)                     
                db1.commit()
            
                sqlConsultaProduto = 'select idproduto from produto where nomeProduto="'+row.descricaoAlimento+'" and idcategoriaDespesa in (28) and idsubcategoriaDespesa=54'			                      
                cursorConsultaProduto = db1.cursor(dictionary=True)
                cursorConsultaProduto.execute(sqlConsultaProduto)     
                records = cursorConsultaProduto.fetchall()              	
          
            produtoControlMoney = Produto(records[0]['idproduto'])            
            idProduto = produtoControlMoney.idproduto
            precoCotacao = row.preco   
            sqlDelete='delete from preco where dataPreco='+'"'+  dataHoraCotacao +'"'+' and idproduto='+str(idProduto)+' and idsubcategoriaDespesa=54 and idcategoriaDespesa=28 and idfornecedores=106'        
            cursorDelete = db1.cursor(dictionary=True)
            cursorDelete.execute(sqlDelete)
            db1.commit()
          
            sqlInsertCotacao = 'insert into preco (idproduto,idfornecedores,idcategoriaDespesa,idsubcategoriaDespesa,precoReal,dataPreco,valorCotacaoDolar,precoFreteDolar,precoDolar,precoFreteReal,observacaoCotacaoPreco) values ('+str(idProduto)+',106,28,54,'+'"'+ str(precoCotacao).strip() +'"'+','+'"'+  dataHoraCotacao +'"'+','+'"0.00"'+','+'"0.00"'+','+'"0.00"'+','+'"0.00"'+',"Gerado automaticamente: '+dataHora+' '+time+'"'+');'				       
            #print(sqlInsertCotacao)
            cursorInsert = db1.cursor(dictionary=True)
            cursorInsert.execute(sqlInsertCotacao)
            db1.commit()

    dbControlMoney = mysql.connector.connect(
        
        host="",
        user="",
        passwd="",
        database=""
        
    )

    gravacotacao(dbControlMoney)