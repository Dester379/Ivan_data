# -*- coding: utf-8 -*-
#processing
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
from pandas import ExcelWriter
scaler = StandardScaler()
label = LabelEncoder()
#warnings
import warnings
warnings.filterwarnings('ignore')
from sklearn.externals import joblib
from datetime import datetime
import pymysql.cursors 



credit_id = '8094'

def main(credit_id):
    #Подключиться к базе данных
    def con():
        conn = pymysql.connect(host='192.168.64.1', port=3306, user='i.serov', password='X3*1Uy(F', db='mysql')
        return conn

    df = pd.read_sql('''
    SELECT
        c.id AS credit_id,    
        ibat.*
    FROM br_release_moneyman.credit c
        INNER JOIN br_release_moneyman.credit_risk_filter crf
            ON crf.credit_id = c.id
        INNER JOIN br_release_moneyman.instantor_bank_account iba
            ON iba.instantor_user_details_id = crf.instantor_user_details_id
        INNER JOIN br_release_moneyman.instantor_bank_account_transaction ibat
            ON ibat.instantor_bank_account_id = iba.id
    WHERE c.date_requested>'2017-01-01' and credit_id = '%s'
    ''' % str(credit_id), con=con())
    
    # df = транзакции инстанотора
    def clusters(x):
        if x.find('ENCARGOS') != -1 or x.find('Enc') != -1  or x.find('ITAU') != -1  and x.find('SOB') != -1  or x.find('SOB') != -1  and x.find('MED') != -1  or x.find('SOB') != -1  and x.find('MEDIDA-MULTA') != -1: 
            d = 'BANK CHARGES'
        elif x.find('LIS') != -1 or x.find('LIS/JUROS') != -1 or x.find('RECUPERACAO') != -1:
            d = 'BANK INTERESTS'
        elif x.find('DEB') != -1 and x.find('CESTA') != -1 or x.find('Tarifa') != -1 or x.find('TARIFA') != -1 or x.find('TAR') != -1 or x.find('Taxa') != -1 or x.find('Histórico') != -1 and x.find('Tarifa') != -1 or x.find('Histórico') != -1 and x.find('Tar') != -1 or x.find('Histórico') != -1 and x.find('Serviços') != -1 or x.find('Cart') != -1 or x.find('ANUIDADE') != -1:
            d = 'BANK TAXES'
        elif x.find('DEBITO') != -1 and x.find('AUTOM') != -1 or x.find('DEBITO') != -1 and x.find('AUT') != -1 or x.find('AUT.') != -1 and x.find('DEBITO') != -1 or x.find('DB') != -1 and x.find('AT') != -1:
            d = 'DIRECT DEBIT'
        elif x.find('DEBITO') != -1 and x.find('AUTOM') != -1 and x.find('EMPRESAS') != -1 and x.find('CONVENIADAS') != -1 and x.find('CREFISA') != -1:
            d = 'DIRECT DEBIT CREFISA'
        elif x.find('DEBITO') != -1 and x.find('AUTOM') != -1 and x.find('EMPRESAS') != -1 and x.find('CONVENIADAS') != -1 and x.find('SOROCRED') != -1:
            d = 'DIRECT DEBIT SOROCRED'
        elif x.find('CX.SEGUROS') != -1 or x.find('DEB') != -1 and x.find('AUTOR') != -1 and (x.find('PORTO') != -1 or x.find('LIBERTY') != -1 or x.find('TOKIO') != -1 or x.find('SUL') != -1 or x.find('MITSUI') != -1 or x.find('AZUL') != -1 or x.find('PORTOSEGMONITO') != -1) or x.find('MENSALIDADE') != -1 and x.find('SEGURO') != -1 or x.find('PGTO') != -1 and x.find('PROTECAO') != -1 or x.find('Seg') != -1 or x.find('SEGURO') != -1:
            d = 'INSURANCE'
        elif x.find('CEP') != -1 or x.find('CEP/JUROS') != -1 or x.find('DEB.JUROS') != -1 or x.find('JUROS') != -1 or x.find('Mora') != -1 or x.find('MULTA') != -1:
            d = 'INTERESTS'
        elif x.find('APL') != -1 or x.find('Apl.invest') != -1 or x.find('APLIC') != -1 or x.find('Aplic.em') != -1 and x.find('Papeis') != -1 or x.find('APLICACAO') != -1 or x.find('DEB') != -1 and x.find('CDC') != -1 or x.find('DEBITO') != -1 and x.find('CAPITALIZACAO') != -1 or x.find('Histórico') != -1 and x.find('Ourocap') != -1 or x.find('Histórico') != -1 and x.find('Aplicação') != -1 or x.find('PIC') != -1 or x.find('PREMIO') != -1 and x.find('VGBL') != -1 or x.find('PREST') != -1 and x.find('CDC') != -1:
            d = 'INVESTMENT'
        elif x.find('DEB.IOF') != -1 or x.find('Histórico') != -1 and x.find('I.O.F.') != -1 or x.find('IOF') != -1:
            d = 'IOF'
        elif x.find('Pagto') != -1 and x.find('Cobranca') != -1 and x.find('Agiplan') != -1:
            d = 'LOAN AGIPLAN'
        elif x.find('Bx.ant.fin/emp') != -1 or x.find('LIQUIDACAO') != -1 or x.find('Histórico') != -1  and x.find('Empréstimo') != -1 or x.find('EMPRESTIMO') != -1 and x.find('CONSIGNADO') != -1 or x.find('CREDITO') != -1 and x.find('CONSIGNADO') != -1 or x.find('PARCELA') != -1 and x.find('CONSIGNADO') != -1 or x.find('PREST') != -1 and x.find('EMPREST') != -1 or x.find('PREST') != -1 and x.find('EMPR') != -1 or x.find('PREST') != -1 and x.find('HAB') != -1 or x.find('PREST') != -1 and x.find('EMPRESTIMOS/FINANCIAMENTOS') != -1 or x.find('DEB') != -1 and x.find('AUTOR') != -1 and x.find('CETELEM') != -1 or x.find('Pagto') != -1 and x.find('Cobranca') != -1 and x.find('Acordo') != -1 and x.find('Cetelem') != -1:
            d = 'LOAN BANK'
        elif x.find('Histórico') != -1 and x.find('BV') != -1:
            d = 'LOAN BV'
        elif x.find('Pagto') != -1 and x.find('Cobranca') != -1 and x.find('Crefisa') != -1 or x.find('DB') != -1 and x.find('CREFISA') != -1 or x.find('DEB') != -1 and x.find('AUTOR') != -1 and x.find('CREFISA') != -1 or x.find('Crefisa') != -1:
            d = 'LOAN CREFISA'   
        elif x.find('Pagto') != -1 and x.find('Cobranca') != -1 and x.find('Geru') != -1 or x.find('Geru') != -1:
            d = 'LOAN GERU'  
        elif x.find('Pagto') != -1 and x.find('Cobranca') != -1 and x.find('Jbcred') != -1 or x.find('Jbcred') != -1:
            d = 'LOAN JBCRED'   
        elif x.find('Pagto') != -1 and x.find('Cobranca') != -1 and x.find('Omni') != -1 or x.find('Omni') != -1:
            d = 'LOAN OMNI'   
        elif x.find('Pagto') != -1 and x.find('Cobranca') != -1 and x.find('Simplic') != -1 or x.find('Simplic') != -1:
            d = 'LOAN SIMPLIC' 
        elif x.find('Pagto') != -1 and x.find('Cobranca') != -1  and x.find('Sorocred') != -1  or x.find('DEB') != -1  and x.find('AUTOR') != -1  and x.find('SOROCRED') != -1  or x.find('Sorocred') != -1:
            d = 'LOAN SOROCRED'
        elif x.find('PAGAMENTO') != -1  and x.find('CONTA') != -1  and x.find('CELULAR') != -1 :
            d = 'MOBILE BILL'
        elif x.find('Pague') != -1  and x.find('Recarga') != -1  or x.find('RECARGA') != -1 :
            d = 'MOBILE CREDITS'
        elif x.find('Prev-seg.vida') != -1  or x.find('DEBITO') != -1  and x.find('PREVIDENCIA') != -1 :
            d = 'RETIRENMENT'
        elif x.find('Histórico') != -1  and x.find('Aplicação') != -1  and x.find('Poupança') != -1  or x.find('APLIC') != -1  and x.find('POUP') != -1  or x.find('TRANSFERENCIA') != -1  and x.find('POUPANCA') != -1 :
            d = 'SAVINGS'
        elif x.find('DOC') != -1 or x.find('DOC/TED') != -1 or x.find('Doc/tedinternet') != -1 or x.find('EMISSAO') != -1  and x.find('DE') != -1  and x.find('DOC') != -1  or x.find('ENVIO') != -1  and x.find('TED') != -1  or x.find('Histórico') != -1  and x.find('TRANSF.RECURSO') != -1  or x.find('TBI') != -1  or x.find('TED') != -1  or x.find('Ted-e') != -1  or x.find('Ted-t') != -1 or x.find('TEF') != -1  or x.find('TEL') != -1  and x.find('TED') != -1  or x.find('TEV') != -1  and x.find('MESM') != -1  or x.find('TRANSF.RECURSO(E/') != -1  or x.find('TRANSF') != -1  or x.find('TRANSFERENCIA') != -1:
            d = 'TRANSFER'
        elif x.find('DEBITO') != -1 and x.find('VALE-TRANSPORTE') != -1:
            d = 'TRANSPORT'
        elif x.find('SAQUE') != -1  or x.find('SAQUETERMINAL') != -1  or x.find('ENVIO') != -1  and x.find('TEV') != -1  or x.find('CP') != -1  and x.find('ELECTRO') != -1  or x.find('SAQUECORRESPONDEN') != -1  or x.find('RETIRADA') != -1  or x.find('SQ') != -1  and x.find('CX') != -1  or x.find('SAQ') != -1  or x.find('RECIBO') != -1  and x.find('RETIRADA') != -1  or x.find('SAQUEPESSOAL') != -1  or x.find('CXE') != -1  and x.find('SAQUE') != -1  or x.find('Histórico') != -1  and x.find('Banco') != -1  and x.find('24') != -1 :
            d = 'WITHDRAWAL'
        #end of negative
        elif x.find('BOLSA') != -1 and x.find('FAMI') != -1:
            d = 'BOLSA FAMILIA'
        elif x.find('ESTORNO') != -1 or x.find('Histórico') != -1 and x.find('Estorno') != -1 or x.find('EST') != -1 or x.find('DOC') != -1 and x.find('DEVOLVIDO') != -1 or x.find('DEVOLUCAO') != -1 or x.find('DEVOL.') != -1 or x.find('DEVOLUC') != -1:
            d = 'CHARGEBACK'
        elif x.find('CHEQUE') != -1 and x.find('DEVOLVIDO') != -1 or x.find('Histórico') != -1 and x.find('Cheque') != -1 and x.find('Devolv') != -1 or x.find('Dev') != -1 and x.find('ch') != -1 or x.find('CH') != -1 and x.find('DEV') != -1 or x.find('DEV') != -1 and x.find('SEM') != -1 and x.find('FUNDOS') != -1:
            d = 'CHECK RETURN'
        elif x.find('dp') != -1 or x.find('depósito') != -1 or x.find('DEP') != -1  or x.find('Dep') != -1 or x.find('CEI') != -1 and x.find('DINHEIRO') != -1 or x.find('Dep') != -1 and x.find('Dinheiro') != -1 or x.find('Dep') != -1 and x.find('c/c') != -1 or x.find('DEP.DINH.') != -1 or x.find('Depos') != -1 or x.find('DEPOSITO') != -1 or x.find('Deposito') != -1 or x.find('DP') != -1 or x.find('TEC') != -1 and x.find('DEP') != -1 or x.find('TEC') != -1 and x.find('DEPOSITO') != -1 or x.find('DEPOSITO') != -1 and x.find('DINHEIRO') != -1:
            d = 'DEPOSIT'
        elif x.find('CRED') != -1 and x.find('FGTS') != -1 or x.find('PAGAMENTO') != -1 and x.find('BENEFICIOS') != -1 or x.find('Histórico') != -1 and x.find('Resgate') != -1 and x.find('Fundo') != -1:
            d = 'FGTS'
        elif x.find('CRED') != -1 and x.find('INSS') != -1 or x.find('Credito') != -1 and x.find('Inss') != -1:
            d = 'INSS'
        elif x.find('CRED') != -1 and x.find('JUROS') != -1:
            d = 'INTERESTS RETURN'
        elif x.find('FT') != -1 or x.find('PIC') != -1 or x.find('REND') != -1 or x.find('RES') != -1 or x.find('Resg.mer.aberto') != -1 or x.find('Resg.tit.capit.') != -1 or x.find('RESGATE') != -1 or x.find('Resgate') != -1 or x.find('REMUNERACAO') != -1 and x.find('CONTAMAX') != -1 or x.find('Resg') != -1 and x.find('Invest') != -1 or x.find('Resg') != -1 and x.find('Investplus') != -1:
            d = 'INVESTMENT'
        elif x.find('TED') != -1 and x.find('Alfa') != -1 or x.find('Alfa') != -1:
            d = 'LOAN ALFA'
        elif x.find('CONTRATACAO') != -1 or x.find('EMPR') != -1 or x.find('Emprest') != -1 or x.find('REST') != -1 or x.find('SOB') != -1 or x.find('CRED') != -1 and x.find('EMPR') != -1 or x.find('CREDITO') != -1 and x.find('CONTRATADO') != -1 or x.find('CREDITO') != -1 and x.find('CONSIGNADO') != -1 or x.find('CREDITO') != -1 and x.find('ITAUCOR') != -1:
            d = 'LOAN BANK'
        elif x.find('TED') != -1 and x.find('BV') != -1 or x.find('TED') != -1 and x.find('655.0001BV') != -1 or x.find('BV') != -1:
            d = 'LOAN BV'
        elif x.find('TED') != -1 and x.find('CETELEM') != -1 or x.find('CETELEM') != -1:
            d = 'LOAN CETELEM'
        elif x.find('TED') != -1 and x.find('COOPERATIVA') != -1:
            d = 'LOAN COOP'
        elif x.find('TED') != -1 and x.find('CREFISA') != -1 or x.find('CREFISA') != -1:
            d = 'LOAN CREFISA'
        elif x.find('TED') != -1 and x.find('JBCRED') != -1:
            d = 'LOAN JBCRED'
        elif x.find('TED') != -1 and x.find('LECCA') != -1 or x.find('LECCA') != -1:
            d = 'LOAN LECCA'
        elif x.find('TED') != -1 and x.find('OMNI') != -1 or x.find('OMNI') != -1:
            d = 'LOAN OMNI'
        elif x.find('SISPAG') != -1 and x.find('SOCINAL') != -1 or x.find('SOCINAL') != -1:
            d = 'LOAN SOCINAL'
        elif x.find('SISPAG') != -1 and x.find('SOROCRED') != -1 or x.find('SOROCRED') != -1 or x.find('Sorocred') != -1 or x.find('Receb') != -1 and x.find('Sorocred') != -1:
            d = 'LOAN SOROCRED'
        elif x.find('Brad') != -1 or x.find('CAIXA') != -1 and x.find('PREV') != -1 or x.find('CREDITO') != -1 and x.find('PREVIDENCIA') != -1 or x.find('Histórico') != -1 and x.find('Brasilprev') != -1 or x.find('Histórico') != -1 and x.find('PREVI') != -1 or x.find('Histórico') != -1 and x.find('PREVIDENCIA') != -1:
            d = 'RETIRENMENT'
        elif x.find('Trans Sal') != -1 or x.find('Ted Csal') != -1 or x.find('SALARIO') != -1 or x.find('ADIANTAMENTO') != -1 and x.find('SALARIO') != -1   or x.find('CONSOLIDACAO') != -1   and x.find('SALARIO') != -1   or x.find('Cred') != -1   and x.find('Salario') != -1   or x.find('Credt') != -1   and x.find('Salario') != -1   or x.find('CT') != -1   and x.find('SALARIO') != -1   or x.find('PAGTO') != -1   and x.find('SALARIO') != -1   or x.find('TEC') != -1   and x.find('SALARIO') != -1   or x.find('REMUNERACAO/SALARIO') != -1   or x.find('LIQUIDO') != -1   and x.find('VENCIMENTO') != -1   or x.find('PAGTO') != -1   and x.find('ADIANT') != -1   or x.find('FOLHA') != -1:
            d = 'SALARY'
        elif x.find('bx') != -1 or x.find('Histórico') != -1and x.find('Resgate') != -1  and x.find('Poupança') != -1  or x.find('Histórico') != -1  and x.find('Resgate') != -1  and x.find('Fundo') != -1  or x.find('INT') != -1  and x.find('RESGATE') != -1  or x.find('REM') != -1  and x.find('BASICA') != -1  or x.find('REMUNER') != -1  and x.find('BASICA') != -1  or x.find('Rendimentos') != -1  and x.find('Poup') != -1  or x.find('RESG') != -1  and x.find('POUP') != -1  or x.find('Resgate') != -1  and x.find('Fundos') != -1:
            d = 'SAVINGS'
        elif x.find('CRED') != -1 and x.find('TED') != -1 or x.find('DOC') != -1 and x.find('ELET') != -1 or x.find('TBI') != -1 or x.find('TRANSFERENCIA') != -1  or x.find('Transf') != -1  or x.find('AG.') != -1  and x.find('TEF') != -1  or x.find('CEI') != -1  and x.find('TEF') != -1  or x.find('CXE') != -1  and x.find('TEF') != -1  or x.find('DEC') != -1  and x.find('TEF') != -1  or x.find('Deb') != -1  and x.find('Transf') != -1  or x.find('DI') != -1  and x.find('DIN') != -1  or x.find('DOC') != -1  and x.find('RECEBIDO-TIT') != -1  or x.find('Doc') != -1  and x.find('Cred') != -1  or x.find('Doc') != -1  and x.find('Cred.autom*') != -1:
            d = 'TRANSFER'
        elif x.find('PARSEG-DES') != -1:
            d = 'UNEMPLOYMENT SUBSIDARY'
        #I made:
        elif x.find('CRED') != -1 or x.find('CRED TEV') != -1 or x.find('Crédito') != -1:
            d = 'Credit'
        elif x.find('CREDIARIO') != -1 or x.find('Estorno de Débito') != -1 or x.find('PAGTO DEV_CONTR') != -1:
            d = 'Refund' 
        elif x.find('Depósito Online') != -1:
            d = 'Deposit Online'
        elif x.find('FUND.COORD.DE') != -1:
            d = 'Student scholarship'  
        elif x.find('COMPRA INTER') != -1:
            d = 'International purchase' 
        elif x.find('fake transaction') != -1 or x.find('Histórico: Saldo Anterior, Documento: null') != -1 or x.find('INT PRE-PAGOXXXXX') != -1 or x.find('INT PAG TIT') != -1:
            d = 'Error'   
        else:
            d = 'Other'
        return d 
    
    def transponir_new(df):
            #columns pos
            poscols = ['credit_id','BANK CHARGES', 'BANK INTERESTS', 'BANK TAXES',
                   'BOLSA FAMILIA', 'CHARGEBACK', 'CHECK RETURN', 'Credit', 'DEPOSIT',
                   'DIRECT DEBIT', 'FGTS', 'INSS', 'INSURANCE', 'INTERESTS', 'INVESTMENT',
                   'IOF', 'International purchase', 'LOAN ALFA', 'LOAN BANK', 'LOAN BV','LOAN CREFISA',
                   'LOAN GERU', 'LOAN JBCRED', 'LOAN LECCA', 'LOAN OMNI', 'LOAN SIMPLIC','LOAN SOCINAL',
                   'LOAN SOROCRED', 'MOBILE CREDITS','Other', 'RETIRENMENT', 'Refund', 'SALARY', 'SAVINGS',
                   'Student scholarship', 'TRANSFER', 'UNEMPLOYMENT SUBSIDARY',
                   'WITHDRAWAL']
            negcols = ['credit_id','BANK CHARGES', 'BANK INTERESTS', 'BANK TAXES',
                   'CHARGEBACK', 'CHECK RETURN', 'Credit', 'DEPOSIT', 'DIRECT DEBIT',
                   'INSURANCE', 'INTERESTS', 'INVESTMENT', 'IOF', 'International purchase',
                   'LOAN AGIPLAN', 'LOAN ALFA', 'LOAN BANK', 'LOAN BV', 'LOAN CETELEM',
                   'LOAN CREFISA', 'LOAN GERU', 'LOAN JBCRED', 'LOAN OMNI', 'LOAN SIMPLIC',
                   'LOAN SOROCRED', 'MOBILE BILL', 'MOBILE CREDITS', 'Other',
                   'RETIRENMENT', 'SALARY', 'SAVINGS', 'TRANSFER', 'TRANSPORT',
                   'WITHDRAWAL']
            #df make summ positive
            df_sp = df[['credit_id', 'id', 'cluster', 'amount']][df.balance_type == 'pos']
            #считаем общую сумму по каждому кластеру и клиенту
            df_sp = df_sp.groupby(['credit_id', 'cluster']).agg({'amount': 'sum'})
            df_sp.reset_index(level=['credit_id', 'cluster'], inplace=True)
            df_sp = df_sp.pivot(index='credit_id', columns='cluster', values='amount').reset_index()
            q_pos = pd.DataFrame(columns=poscols)
            df_sp = pd.concat([q_pos, df_sp], axis=0, join='outer')
            df_sp.columns = 'sum_pos_' + df_sp.columns
            df_sp = df_sp.rename(columns = {'sum_pos_credit_id':'credit_id'})
    
            #df make summ negative
            df_sn = df[['credit_id', 'id', 'cluster', 'amount']][df.balance_type == 'neg']
            #считаем общую сумму по каждому кластеру и клиенту
            df_sn = df_sn.groupby(['credit_id', 'cluster']).agg({'amount': 'sum'})
            df_sn.reset_index(level=['credit_id', 'cluster'], inplace=True)
            df_sn = df_sn.pivot(index='credit_id', columns='cluster', values='amount').reset_index()
            q_neg = pd.DataFrame(columns=negcols)
            df_sn = pd.concat([q_neg, df_sn], axis=0, join='outer')
            df_sn.columns = 'sum_neg_' + df_sn.columns
            df_sn = df_sn.rename(columns = {'sum_neg_credit_id':'credit_id'})
    
            #df make count positive
            df_cp = df[['credit_id', 'id', 'cluster', 'amount']][df.balance_type == 'pos']
            #считаем общую сумму по каждому кластеру и клиенту
            df_cp = df_cp.groupby(['credit_id', 'cluster']).agg({'amount': 'count'})
            df_cp.reset_index(level=['credit_id', 'cluster'], inplace=True)
            df_cp = df_cp.pivot(index='credit_id', columns='cluster', values='amount').reset_index()
            q_pos = pd.DataFrame(columns=poscols)
            df_cp = pd.concat([q_pos, df_cp], axis=0, join='outer')
            df_cp.columns = 'cnt_pos_' + df_cp.columns
            df_cp = df_cp.rename(columns = {'cnt_pos_credit_id':'credit_id'})
    
            #df make count negative
            df_cn = df[['credit_id', 'id', 'cluster', 'amount']][df.balance_type == 'neg']
            #считаем общую сумму по каждому кластеру и клиенту
            df_cn = df_cn.groupby(['credit_id', 'cluster']).agg({'amount': 'count'})
            df_cn.reset_index(level=['credit_id', 'cluster'], inplace=True)
            df_cn = df_cn.pivot(index='credit_id', columns='cluster', values='amount').reset_index()
            q_neg = pd.DataFrame(columns=negcols)
            df_cn = pd.concat([q_neg, df_cn], axis=0, join='outer')
            df_cn.columns = 'cnt_neg_' + df_cn.columns
            df_cn = df_cn.rename(columns = {'cnt_neg_credit_id':'credit_id'})
    
            df_sum = pd.merge(df_sp, df_sn, on='credit_id', how ='outer').fillna(0).set_index('credit_id')
            cnt = pd.merge(df_cp, df_cn, on='credit_id', how = 'outer').fillna(0)
            return df_sum, cnt
    
    def if_salary(df):
        q = []
        for i in range(len(df)):
            if df.salarie_dates[i].find(str(df.day_of_month_of_spisanie[i])) != -1:
                q.append(1)
            else:
                q.append(0)
        df['is_date_salary'] = q
        return df 
    
    df.description = df.description.map(lambda x: x.replace('Description:', ' '))
    df['cluster'] = df['description'].apply(lambda x: clusters(x))
    
    df['day_of_month_of_tr'] = df.date.map(lambda x: x.day)
    df['day_of_week_of_tr'] = df.date.map(lambda x: x.weekday())
    df['isweekday'] = df.day_of_week_of_tr.map(lambda x: 1 if x==5 or x==6 else 0)
    
    salaries = df[(df.cluster=='SALARY') & (df.amount>=150)][['credit_id', 'day_of_month_of_tr']].drop_duplicates()
    salaries['day_of_month_of_tr'] = salaries['day_of_month_of_tr'].map(lambda x: str(x)+',')
    salaries['salarie_dates'] = salaries.groupby(['credit_id'])['day_of_month_of_tr'].transform('sum')
    salaries = salaries[['credit_id', 'salarie_dates']].drop_duplicates()
    
    df['max_sum'] = df.groupby(['credit_id'])['amount'].transform('max')
    day_of_sal = df[df.max_sum==df.amount][['credit_id', 'day_of_month_of_tr']]
    day_of_sal = day_of_sal.rename(columns={'day_of_month_of_tr':'day_of_max_trans'})
    
    df.drop(['day_of_month_of_tr','day_of_week_of_tr','isweekday'], axis=1, inplace=True)
    
    df = df[df.cluster!='Error']
    df.index = range(len(df.index))
    
    df['min_date_month'] = df.groupby(['credit_id'])['date'].transform('min').apply(lambda x: x.month)
    df['max_date_month'] = df.groupby(['credit_id'])['date'].transform('max').apply(lambda x: x.month)
    df['min_date_day'] = df.groupby(['credit_id'])['date'].transform('min').apply(lambda x: x.day)
    df['max_date_day'] = df.groupby(['credit_id'])['date'].transform('max').apply(lambda x: x.day)
    df['min_date_year'] = df.groupby(['credit_id'])['date'].transform('min').apply(lambda x: x.year)
    df['max_date_year'] = df.groupby(['credit_id'])['date'].transform('max').apply(lambda x: x.year)
    df['count_transactions'] = df.groupby(['credit_id'])['credit_id'].transform('count')
    df['mean_amount'] = df.groupby(['credit_id'])['amount'].transform('mean')
    df['min_amount'] = df.groupby(['credit_id'])['amount'].transform('min')
    df['max_amount'] = df.groupby(['credit_id'])['amount'].transform('max')
    df['median_amount'] = df.groupby(['credit_id'])['amount'].transform('median')
    df['sum_amount'] = df.groupby(['credit_id'])['amount'].transform('sum')
    df['amount_type'] = df.amount.apply(lambda x: 1 if x>0 else 0)
    df['amount_count_pos'] = df.groupby(['credit_id'])['amount_type'].transform('sum')
    df['amount_count_neg'] = df['count_transactions'] - df['amount_count_pos']
    df.drop('amount_type', axis = 1, inplace = True)
    df['mean_balance'] = df.groupby(['credit_id'])['balance'].transform('mean')
    df['min_balance'] = df.groupby(['credit_id'])['balance'].transform('min')
    df['max_balance'] = df.groupby(['credit_id'])['balance'].transform('max')
    df['median_balance'] = df.groupby(['credit_id'])['balance'].transform('median')
    df['sum_balance'] = df.groupby(['credit_id'])['balance'].transform('sum')
    df['balance_type'] = df.balance.apply(lambda x: 1 if x>0 else 0)
    df['balance_count_pos'] = df.groupby(['credit_id'])['balance_type'].transform('sum')
    df['balance_count_neg'] = df['count_transactions'] - df['balance_count_pos']
    df.drop('balance_type', axis = 1, inplace = True)
    df['balance_type'] = df['amount'].map(lambda x: 'pos' if x > 0 else 'neg' )
    
    df['date_day'] = df.date.apply(lambda x: x.day)
    df1 = df.drop(['id', 'date', 'description', 'amount', 'balance', 
                    'cluster', 'balance_type','date_day', 'instantor_bank_account_id'], axis = 1)
    df1 = df1.drop_duplicates().reset_index().drop('index', axis = 1).set_index('credit_id')
    df1['count_distinct_dates'] = df.groupby('credit_id').agg({'date_day': 'nunique'})
    df1['count_distinct_clusters'] = df.groupby('credit_id').agg({'cluster': 'nunique'})
    
    df_sum, cnt = transponir_new(df)
    df2 = pd.merge(df1.reset_index(), df_sum.reset_index(), on='credit_id', how ='outer')
    df3 = pd.merge(df2, cnt, on='credit_id', how ='outer')
    df4 = pd.merge(df3, salaries, on='credit_id', how ='outer')
    
    
    ############# загрузка данных не транзакций в df из json 
    
    df = pd.read_sql('''
    SELECT
        p.credit_id as crd_id,
        date_attempt,
        amount,
        success,
        iba.*,
        cc1.*,
        w.education,w.attested_income,w.employment, w.monthly_expenses,w.loan_reason,
        pd.marital_status,pd.sex,pd.place_of_birth,pd.net_worth,
        i.device_new,i.device_type, i.realipaddress_loc_city, 
        i.ruleset_score,
        day(p.date_attempt)                            AS day_of_month_of_spisanie,
        28 - day(p.date_attempt) as diff_good_day_attempt_day,
        dayofweek(p.date_attempt)                      AS day_of_week_of_spisanie,
        month(p.date_attempt)                      AS month_of_spisanie,
        datediff(day(w.next_salary_date), day(c.date_requested)) AS days_bef_sal,
        datediff(day(w.next_salary_date), day(p.date_attempt))   AS days_bef_income,
        datediff(pd.birthday, c.date_requested) AS age,
        p.amount / cc1.expired_debt                    AS percent_of_debt
    FROM br_release_moneyman.direct_debit_pay_attempt p
        LEFT JOIN br_release_moneyman.credit c
            ON p.credit_id = c.id
        LEFT JOIN br_release_moneyman.credit_calculations cc1
            ON p.credit_id = cc1.credit_id AND date(p.date_attempt) = (cc1.calculation_date + INTERVAL 1 DAY)
        LEFT JOIN br_release_moneyman.borrower b
            ON c.borrower_id = b.id
        LEFT JOIN br_release_moneyman.work w
            ON b.work_id = w.id
        LEFT JOIN br_release_moneyman.personal_data pd
            ON b.personal_data_id = pd.id
        LEFT JOIN br_release_moneyman.iovation i
            ON i.credit_id = c.id
        LEFT JOIN br_release_moneyman.credit_risk_filter crf
            ON c.id = crf.credit_id
        LEFT JOIN br_release_moneyman.instantor_bank_account iba
            ON iba.instantor_user_details_id = crf.instantor_user_details_id
    WHERE p.credit_id = '%s' and success is not NULL and c.date_requested>'2017-01-01' and date_attempt<'2018-03-20' and iba.instantor_user_details_id is not null
    ORDER BY p.credit_id DESC
    ''' % str(credit_id), con=con())
    
    df = pd.merge(df, df4, left_on='credit_id', right_on='credit_id', how ='left')
    df_1 = pd.merge(df, day_of_sal.drop_duplicates(), left_on='credit_id', right_on='credit_id', how ='left')
    df_1['max_sum_to_amount'] = df_1['max_sum']/df_1['amount'].map(lambda x: float(x))
    df_1['is_date_eq_sal_date'] = np.where(df_1['day_of_month_of_spisanie'] == df_1['day_of_max_trans'], 1, 0)
    df_1['dif_inst_sal'] = df_1['day_of_month_of_spisanie'] - df_1['day_of_max_trans']
    df_1.salarie_dates = df_1.salarie_dates.map(lambda x: str(x)).replace('nan', 'Nonenone')
    df = if_salary(df_1)
    to_drop = ['id','iban','holder_name','status', 'calculation_date',
           'instantor_user_details_id','prev_credit_calculation_id', 'currency', 'number']
    df = df.drop(to_drop, axis=1)
    
    cat_vars=['bank_name','education','employment','loan_reason', 'marital_status','place_of_birth',
              'net_worth','realipaddress_loc_city', 'device_type']
    for i in cat_vars:
        df[i]=df[i].map(lambda x: str(x))
        label = joblib.load('BR_DD_labl_%s.pkl'% i)
        try:
            df[i]=label.transform(df[i])
        except:
            df[i]=label.fit_transform(df[i])
    
    df1 = df.copy()    
    df = df.drop(['salarie_dates','credit_id', 'date_attempt'], axis=1)
    df = df.drop(['crd_id','success'], axis=1)
    df = df.fillna(-1)
    clf = joblib.load('BR_DD_alg.pkl')
    df1['predictions'] = clf.predict_proba(df)[:, 1]
    return (df1[['credit_id', 'predictions']])















