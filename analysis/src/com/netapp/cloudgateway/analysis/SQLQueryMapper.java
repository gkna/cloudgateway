package com.gk.cloudgateway.analysis;

import javax.rmi.CORBA.Util;

import com.gk.cloudgateway.model.client.FileInfoDTO;

public class SQLQueryMapper {

	static long convertedTimeInMills;

	public static String getQuery(FileInfoDTO fileDTO) {
		String sqlSelectText = "Select * from filecatelog";

		// convert time unit to actual time value
		long accessTime = 0;
		if (fileDTO.getTimeUnit() != null
				&& !fileDTO.getTimeUnit().trim().isEmpty()) {
			if (fileDTO.getTimeUnit().equals("mins")) {
				convertedTimeInMills = fileDTO.getTime() * 60 * 1000;
			} else if (fileDTO.getTimeUnit().equals("hours")) {
				convertedTimeInMills = fileDTO.getTime() * 60 * 60 * 1000;
			} else if (fileDTO.getTimeUnit().equals("months")) {
				convertedTimeInMills = fileDTO.getTime() * 30 * 24 * 60 * 60
						* 1000;
			} else if (fileDTO.getTimeUnit().equals("years")) {
				convertedTimeInMills = fileDTO.getTime() * 365 * 24 * 60 * 60
						* 1000;
			} else {
				convertedTimeInMills = fileDTO.getTime() * 1000;
			}
			accessTime = (System.currentTimeMillis() - convertedTimeInMills) / 1000;
		}

		String operator = "";
		if (fileDTO.getOperation().equals("lt")) {
			operator = ">";
		} else if (fileDTO.getOperation().equals("le")) {
			operator = ">=";
		} else if (fileDTO.getOperation().equals("gt")) {
			operator = "<";
		} else if (fileDTO.getOperation().equals("ge")) {
			operator = "<=";
		}

		String fileAttributeOperation = null;
		if (fileDTO.getFileAttribute().equals("last_accessed")) {
			fileAttributeOperation = "lastaccess ";
		} else if (fileDTO.getFileAttribute().equals("last_modified")) {
			fileAttributeOperation = "lastwrite ";
		} else if (fileDTO.getFileAttribute().equals("last_read")) {
			fileAttributeOperation = "lastread ";
		}

		if (fileAttributeOperation != null) {
			sqlSelectText += " where " + fileAttributeOperation + operator
					+ accessTime;
		}

		return sqlSelectText;
	}

	/**
	 * For hot-data, we should check all the rows of the db.
	 *
	 * @return
	 */
	public static String getAllRows() {
		String sqlSelectText = "SELECT * from filecatelog";
		return sqlSelectText;
	}

	public static String getQueryForMaxAccessCount() {
		String sqlSelectMaxTest = "Select max(accesscount) from filecatelog";
		return sqlSelectMaxTest;
	}

}
