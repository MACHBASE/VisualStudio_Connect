// ConsoleApplication_x86.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <wchar.h>

#include <machbase_sqlcli.h>

#define MACHBASE_PORT_NO		5656

#define RC_SUCCESS          0
#define RC_FAILURE          -1

#define ERROR_CHECK_COUNT	100

SQLHENV 	gEnv;
SQLHDBC 	gCon;

#define CHECK_STMT_RESULT(aRC, aSTMT, aMsg)     \
    if( sRC != SQL_SUCCESS )                    \
    {                                           \
        printError(gEnv, gCon, aSTMT, aMsg);    \
        goto error;                             \
    }                                        

#define CHECK_APPEND_RESULT(aRC, aEnv, aCon, aSTMT)             \
    if( !SQL_SUCCEEDED(aRC) )                                   \
    {                                                           \
        if( checkAppendError(aEnv, aCon, aSTMT) == RC_FAILURE ) \
        {                                                       \
            return RC_FAILURE;                                  \
        }                                                       \
    } 

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg);
int connectDB();
void disconnectDB();

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg)
{
	SQLINTEGER      sNativeError;
	SQLWCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
	SQLWCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
	SQLSMALLINT     sMsgLength;


	if (aMsg != NULL)
	{
		printf("%s\n", aMsg);
	}

	if (SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
		sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS)
	{
		printf("SQLSTATE-[%ls], Machbase-[%d][%ls]\n", sSqlState, (int)sNativeError, sErrorMsg);
	}
}


int connectDB()
{
	SQLWCHAR sConnStr[1024] = L"DSN=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=5656";

	if (SQLAllocEnv(&gEnv) != SQL_SUCCESS)
	{
		printf("SQLAllocEnv error\n");
		return RC_FAILURE;
	}

	if (SQLAllocConnect(gEnv, &gCon) != SQL_SUCCESS)
	{
		printf("SQLAllocConnect error\n");

		SQLFreeEnv(gEnv);
		gEnv = SQL_NULL_HENV;

		return RC_FAILURE;
	}


	if (SQLDriverConnect(gCon, NULL,
		sConnStr,
		SQL_NTS,
		NULL, 0, NULL,
		SQL_DRIVER_NOPROMPT) != SQL_SUCCESS
		)
	{

		printError(gEnv, gCon, NULL, "SQLDriverConnect error");

		SQLFreeConnect(gCon);
		gCon = SQL_NULL_HDBC;

		SQLFreeEnv(gEnv);
		gEnv = SQL_NULL_HENV;

		return RC_FAILURE;
	}

	return RC_SUCCESS;
}

void disconnectDB()
{
	if (SQLDisconnect(gCon) != SQL_SUCCESS)
	{
		printError(gEnv, gCon, NULL, "SQLDisconnect error");
	}

	SQLFreeConnect(gCon);
	gCon = SQL_NULL_HDBC;

	SQLFreeEnv(gEnv);
	gEnv = SQL_NULL_HENV;
}

void printColumn(char *aCol, SQLLEN aLen, char *aFormat, ...)
{
	fprintf(stdout, "%s : ", aCol);

	if (aLen == SQL_NULL_DATA)
	{
		fprintf(stdout, "NULL");
	}
	else
	{
		va_list ap;
		va_start(ap, aFormat);
		vfprintf(stdout, aFormat, ap);
		va_end(ap);
	}
}

int selectTable()
{
	SQLWCHAR *sSQL = L"SELECT c1, c2, c3, c4 FROM TEST_TABLE WHERE C2 > ? LIMIT 10";

	SQLHSTMT    sStmt = SQL_NULL_HSTMT;
	SQLRETURN   sRC = SQL_ERROR;
	int         i = 0;

	SQLLEN      sC1Len = 0;
	SQLLEN      sC2Len = 0;
	SQLLEN      sC3Len = 0;
	SQLLEN      sC4Len = 0;
	SQLLEN      sP2Len = 0;

	int                  sC1;
	float                sC2;
	SQLWCHAR             sC3[81];
	SQLWCHAR             sC4[241];
	float                sP2 = 120;

	char                 smC3[81];
	char                 smC4[241];
	int                  slen = 0;

	if (SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS)
	{
		printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
		goto error;
	}

	if (SQLPrepare(sStmt, (SQLWCHAR *)sSQL, SQL_NTS) != SQL_SUCCESS)
	{
		printError(gEnv, gCon, sStmt, "SQLPrepare Error");
		goto error;
	}

	if (SQLBindParameter(sStmt,
		1,
		SQL_PARAM_INPUT,
		SQL_C_FLOAT,
		SQL_FLOAT,
		0,
		0,
		&sP2,
		0,
		&sP2Len) != SQL_SUCCESS)
	{
		printError(gEnv, gCon, sStmt, "SQLBindParameter 1 Error");
		goto error;
	}

	if (SQLExecute(sStmt) != SQL_SUCCESS)
	{
		printError(gEnv, gCon, sStmt, "SQLExecute Error");
		goto error;
	}

	sRC = SQLBindCol(sStmt, 1, SQL_C_LONG, &sC1, 0, &sC1Len);
	CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 1 Error");

	sRC = SQLBindCol(sStmt, 2, SQL_C_FLOAT, &sC2, 0, &sC2Len);
	CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 2 Error");

	sRC = SQLBindCol(sStmt, 3, SQL_C_WCHAR, &sC3, sizeof(sC3), &sC3Len);
	CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 3 Error");

	sRC = SQLBindCol(sStmt, 4, SQL_C_WCHAR, &sC4, sizeof(sC4), &sC4Len);
	CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 4 Error");

	fprintf(stdout, "SQL : '%ls' <----- parameter=%f\n", sSQL, sP2);
	while (SQLFetch(sStmt) == SQL_SUCCESS)
	{
		printf("===== %d ========\n", i++);
		slen = WideCharToMultiByte(949, 0, sC3, -1, NULL, 0, NULL, NULL);
		WideCharToMultiByte(949, 0, sC3, -1, smC3, slen, NULL, NULL);
		slen = WideCharToMultiByte(949, 0, sC4, -1, NULL, 0, NULL, NULL);
		WideCharToMultiByte(949, 0, sC4, -1, smC4, slen, NULL, NULL);

		printColumn("[C1", sC1Len, "%d", sC1);
		printColumn(", C2", sC2Len, "%f", sC2);
		printColumn(", C3", sC3Len, "%s", smC3);
		printColumn(", C4", sC4Len, "%s", smC4);
		printf("]\n");

	}

	if (SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS)
	{
		printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
		goto error;
	}
	sStmt = SQL_NULL_HSTMT;

	return RC_SUCCESS;

error:
	if (sStmt != SQL_NULL_HSTMT)
	{
		SQLFreeStmt(sStmt, SQL_DROP);
		sStmt = SQL_NULL_HSTMT;
	}

	return RC_FAILURE;
}

int checkAppendError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt)
{
	SQLINTEGER      sNativeError;
	SQLWCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
	SQLWCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
	SQLSMALLINT     sMsgLength;

	if (SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
		sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) != SQL_SUCCESS)
	{
		return RC_FAILURE;
	}

	printf("SQLSTATE-[%ls], Machbase-[%ld][%ls]\n", sSqlState, sNativeError, sErrorMsg);

	if (sNativeError != 9604 &&
		sNativeError != 9605 &&
		sNativeError != 9606)
	{
		return RC_FAILURE;
	}

	return RC_SUCCESS;
}

int appendOpen(SQLHSTMT aStmt)
{
	SQLWCHAR *sTableName = L"TEST_TABLE";

	if (SQLAppendOpenW(aStmt, sTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS)
	{
		printError(gEnv, gCon, aStmt, "SQLAppendOpen Error");
		return RC_FAILURE;
	}

	return RC_SUCCESS;
}

int appendData(SQLHSTMT aStmt)
{
	SQL_APPEND_PARAM sParam[5];
	SQLRETURN        sRC;

	SQLWCHAR         sVarchar2[100] = { 0, };
	SQLWCHAR         sVarchar3[200] = { 0, };

	memset(sParam, 0, sizeof(sParam));

	for (int i = 0; i < 10; i++)
	{
		swprintf_s(sVarchar2, 100, L"MY VARCHAR - %d", i);
		swprintf_s(sVarchar3, 200, L"ÇÑ±Û VARCHAR TEST(2) - %d", i);

		sParam[0].mInteger = 4200 + i;
		sParam[1].mFloat = (float)400 + i;

		sParam[2].mVar.mLength = (unsigned int)wcslen(sVarchar2) * 2;
		sParam[2].mVar.mData = sVarchar2;

		sParam[3].mVar.mLength = (unsigned int)wcslen(sVarchar3) * 2;
		sParam[3].mVar.mData = sVarchar3;

		sParam[4].mDateTime.mTime = SQL_APPEND_DATETIME_NULL;

		sRC = SQLAppendDataV2(aStmt, sParam);
		CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);
	}

	return RC_SUCCESS;
}

int appendClose(SQLHSTMT aStmt)
{
	long long int sSuccessCount = 0;
	long long int sFailureCount = 0;

	if (SQLAppendClose(aStmt, &sSuccessCount, &sFailureCount) != SQL_SUCCESS)
	{
		printError(gEnv, gCon, aStmt, "SQLAppendClose Error");
		return RC_FAILURE;
	}

	printf("success : %ld, failure : %ld\n", (long)sSuccessCount, (long)sFailureCount);

	return (int)sSuccessCount;
}



int _tmain(int argc, _TCHAR* argv[])
{
	SQLHSTMT    sStmt = SQL_NULL_HSTMT;

	if (connectDB() == RC_SUCCESS)
	{
		printf("connectDB success.\n");

		if (SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS)
		{
			printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
			return RC_FAILURE;
		}
		if (appendOpen(sStmt) != RC_SUCCESS)
		{
			return RC_FAILURE;
		}
		printf("Append opened.\n");

		appendData(sStmt);

		if (appendClose(sStmt) == RC_FAILURE)
		{
			return RC_FAILURE;
		}
		printf("Append closed.\n");

		selectTable();

		disconnectDB();
	}
	else
	{
		printf("connectDB failure.\n");
		return RC_FAILURE;
	}

	return RC_SUCCESS;
}

