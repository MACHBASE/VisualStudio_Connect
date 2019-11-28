
#include "stdafx.h"
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
int createTable();
int connectDB();
void disconnectDB();

int createTable()
{
	SQLCHAR sSQL[1024] = "CREATE TABLE TEST_TABLE(C1 integer, C2 float, C3 varchar(40), C4 varchar(120), C5 datetime)";

	SQLHSTMT    sStmt = SQL_NULL_HSTMT;
	SQLRETURN   sRC = SQL_ERROR;

	if (SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS)
	{
		printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
		goto error;
	}

	if (SQLExecDirect(sStmt, sSQL, SQL_NTS) != SQL_SUCCESS)
	{

		printError(gEnv, gCon, sStmt, "SQLExecDirect Error");
		goto error;
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

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg)
{
	SQLINTEGER      sNativeError;
	SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
	SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
	SQLSMALLINT     sMsgLength;


	if (aMsg != NULL)
	{
		printf("%s\n", aMsg);
	}

	if (SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
		sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS)
	{
		printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, (int)sNativeError, sErrorMsg);
	}
}


int connectDB()
{
	SQLCHAR sConnStr[1024] = "DSN=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=5656";

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

	//sprintf_s(sConnStr, sizeof(sConnStr), "DSN=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d", MACHBASE_PORT_NO);

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
	const char *sSQL = "SELECT c1, c2, c3, c4 FROM TEST_TABLE WHERE C2 > ? LIMIT 10";
	//	SQLWCHAR *sSQL = L"SELECT c1, c2, c3, c4 FROM TEST_TABLE WHERE C2 > ? LIMIT 10";

	SQLHSTMT    sStmt = SQL_NULL_HSTMT;
	SQLRETURN   sRC = SQL_ERROR;
	int         i = 0;

	SQLLEN      sC1Len = 0;
	SQLLEN      sC2Len = 0;
	SQLLEN      sC3Len = 0;
	SQLLEN      sC4Len = 0;
	SQLLEN      sP2Len = 0;

	int			sC1;
	float       sC2;
	SQLCHAR     sC3[81];
	SQLCHAR     sC4[241];
	float       sP2 = 120;

	int                  slen = 0;

	if (SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS)
	{
		printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
		goto error;
	}

	if (SQLPrepare(sStmt, (SQLCHAR *)sSQL, SQL_NTS) != SQL_SUCCESS)
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

	sRC = SQLBindCol(sStmt, 3, SQL_C_CHAR, &sC3, sizeof(sC3), &sC3Len);
	CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 3 Error");

	sRC = SQLBindCol(sStmt, 4, SQL_C_CHAR, &sC4, sizeof(sC4), &sC4Len);
	CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 4 Error");

	fprintf(stdout, "SQL : '%s' <----- parameter=%f\n", sSQL, sP2);
	//	wprintf(L"SQL : '%s'  parameter=%f\n", sSQL, sP2);
	while (SQLFetch(sStmt) == SQL_SUCCESS)
	{
		printf("===== %d ========\n", i++);
		printColumn("[C1", sC1Len, "%d", sC1);
		printColumn(", C2", sC2Len, "%f", sC2);
		printColumn(", C3", sC3Len, "%s", sC3);  // 결과에 한글이 있으면 사용불가
		printColumn(", C4", sC4Len, "%s", sC4);

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
	SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
	SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
	SQLSMALLINT     sMsgLength;

	if (SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
		sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) != SQL_SUCCESS)
	{
		return RC_FAILURE;
	}

	printf("SQLSTATE-[%s], Machbase-[%ld][%s]\n", sSqlState, sNativeError, sErrorMsg);

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
	char *sTableName = "TEST_TABLE";

	if (SQLAppendOpen(aStmt, (SQLCHAR *)sTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS)
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

	char         sVarchar2[100] = { 0, };
	char         sVarchar3[200] = { 0, };

	memset(sParam, 0, sizeof(sParam));

	for (int i = 0; i < 10; i++)
	{

		sprintf_s(sVarchar2, 100, "MY VARCHAR - %d", i);
		sprintf_s(sVarchar3, 200, "한글테스트 VARCHAR TEST(2) - %d", i);

		sParam[0].mInteger = 5200 + i;
		sParam[1].mFloat = (float)500 + i;

		sParam[2].mVar.mLength = (unsigned int)strlen(sVarchar2);
		sParam[2].mVar.mData = sVarchar2;

		sParam[3].mVar.mLength = (unsigned int)strlen(sVarchar3);
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

	return (long)sSuccessCount;
}



int _tmain(int argc, _TCHAR* argv[])
{
	SQLHSTMT    sStmt = SQL_NULL_HSTMT;

	if (connectDB() == RC_SUCCESS)
	{
		printf("connectDB success.\n");

		createTable();

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

