package com.netapp.cloudgateway.analysis;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.netapp.cloudgateway.model.client.FileInfoDTO;
import com.netapp.cloudgateway.model.client.FileInfoOutputModel;
import com.netapp.cloudgateway.model.server.MySqlDataBaseConnector;

public class AnyalysisEngine {
	Connection connection;
	static int accessCount;
	static int accessCountRatio;

	private static final AnyalysisEngine instance = new AnyalysisEngine();

	public static AnyalysisEngine getInstance() {
		return instance;
	}

	private AnyalysisEngine() {
		connection = MySqlDataBaseConnector.getConnection();
	}

	public List<FileInfoOutputModel> retreveData(FileInfoDTO fileDTO) {
		long totalFilesSizeForBackup = 0;
		ArrayList<FileInfoOutputModel> outputList = new ArrayList<FileInfoOutputModel>();
		String query = SQLQueryMapper.getQuery(fileDTO);
		System.out.println("Query is retrieving 1: " + query);
		try {
			Statement stm = connection.createStatement();
			ResultSet executeQuery = stm.executeQuery(query);
			while (executeQuery.next()) {
				FileInfoOutputModel model = new FileInfoOutputModel();

				String fileName = executeQuery.getString("filename");
				long fileSize = executeQuery.getLong("filesize");
				totalFilesSizeForBackup = totalFilesSizeForBackup + fileSize;
				System.out.println(fileSize);
				model.setFileName(fileName);
				model.setFileSize(fileSize);
				model.setTotalFileSize(totalFilesSizeForBackup);
				outputList.add(model);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return outputList;
	}

	public List<FileInfoOutputModel> retreveDataForHotFiles(FileInfoDTO fileDTO) {
		int totalFiles = 0;
		int totalhotFiles = 0;
		long totalFilesSize = 0;
		long totalHotFileSize = 0;

		ArrayList<FileInfoOutputModel> outputList = new ArrayList<FileInfoOutputModel>();
		String query = SQLQueryMapper.getQueryForMaxAccessCount();
		System.out.println("Hot:Cold related query is retrieving: " + query);
		try {
			Statement stm = connection.createStatement();
			ResultSet executeQuery = stm.executeQuery(query);

			while (executeQuery.next()) {
				accessCount = executeQuery.getInt(1);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		// Calculate the readcountLimt
		accessCountRatio = ((accessCount * fileDTO.getHotfilePercentile()) / 100);

		System.out.println(fileDTO.getHotfilePercentile() + " of "
				+ accessCount + " is " + accessCountRatio);
		// Get the list of the files
		// String queryForFiles = SQLQueryMapper.getQuery(fileDTO);
		String queryForFiles = SQLQueryMapper.getAllRows();
		System.out.println("getAllRows for hot-data: " + queryForFiles);
		try {
			Statement stm1 = connection.createStatement();
			ResultSet executeQuery = stm1.executeQuery(queryForFiles);
			while (executeQuery.next()) {
				FileInfoOutputModel model = new FileInfoOutputModel();

				String fileName = executeQuery.getString("filename");
				long fileSize = executeQuery.getLong("filesize");
				long fileAccessedCount = executeQuery.getLong("accesscount");
				model.setFileName(fileName);
				model.setFileSize(fileSize);
				if (fileAccessedCount >= accessCountRatio) {
					model.setIsFileHot(true);
					totalhotFiles++;
					totalHotFileSize = totalHotFileSize + fileSize;
				} else {
					model.setIsFileHot(false);
				}
				totalFiles++;
				totalFilesSize = totalFilesSize + fileSize;

				model.setHotFileRatio(totalhotFiles + ":" + totalFiles);
				model.setTotalFileSize(totalFilesSize);
				model.setTotalHotFileSize(totalHotFileSize);

				outputList.add(model);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return outputList;
	}

	public static void main(String args[]) {
		FileInfoDTO dto = new FileInfoDTO();
		dto.setFileAttribute("last_accessed");
		dto.setOperation("lt");
		dto.setTimeUnit("mins");
		dto.setTime(5L);

		AnyalysisEngine.getInstance().retreveData(dto);
	}

}
