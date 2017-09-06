/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.common.ftp;

import org.apache.commons.net.ftp.FTP;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Logger;

/**
 * Connects to {@link FTPServer} in order to upload or download a file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class FTPClient {

  private final Logger logger;

  public FTPClient(Logger logger) {
    this.logger = logger;
  }

  public void uploadFile(File fileToUpload, String ipAddress, String port) {
    uploadFile(fileToUpload, ipAddress, port, FTPServer.DEFAULT_USER_NAME,
            FTPServer.DEFAULT_PASSWORD);
  }

  public void uploadFile(File fileToUpload, String ipAddress, String port, String username,
          String password) {
    org.apache.commons.net.ftp.FTPClient ftpClient = new org.apache.commons.net.ftp.FTPClient();
    try {
      ftpClient.connect(ipAddress, Integer.parseInt(port));
      ftpClient.login(username, password);
      ftpClient.enterLocalPassiveMode();

      ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

      String remoteFile = fileToUpload.getName();
      try (InputStream inputStream = new BufferedInputStream(new FileInputStream(fileToUpload));) {
        boolean done = ftpClient.storeFile(remoteFile, inputStream);
        logServerReply(ftpClient);
        if (!done) {
          throw new RuntimeException(
                  "The file " + fileToUpload.getAbsolutePath() + " could not be uploaded.");
        }
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        if (ftpClient.isConnected()) {
          ftpClient.logout();
          ftpClient.disconnect();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void downloadFile(String remoteFile, File localFile, String ipAddress, String port) {
    downloadFile(remoteFile, localFile, ipAddress, port, FTPServer.DEFAULT_USER_NAME,
            FTPServer.DEFAULT_PASSWORD);
  }

  public void downloadFile(String remoteFile, File localFile, String ipAddress, String port,
          String username, String password) {
    org.apache.commons.net.ftp.FTPClient ftpClient = new org.apache.commons.net.ftp.FTPClient();
    try {
      ftpClient.connect(ipAddress, Integer.parseInt(port));
      ftpClient.login(username, password);
      ftpClient.enterLocalPassiveMode();

      ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

      try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localFile));) {
        boolean done = ftpClient.retrieveFile(remoteFile, outputStream);
        logServerReply(ftpClient);
        if (!done) {
          throw new RuntimeException("The file " + remoteFile + " could not be downloaded.");
        }
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        if (ftpClient.isConnected()) {
          ftpClient.logout();
          ftpClient.disconnect();
        }
      } catch (IOException e) {
        if (logger != null) {
          logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
                  e);
        }
      }
    }
  }

  private void logServerReply(org.apache.commons.net.ftp.FTPClient ftpClient) {
    String[] replies = ftpClient.getReplyStrings();
    if ((replies != null) && (replies.length > 0)) {
      for (String aReply : replies) {
        if (logger != null) {
          logger.fine("SERVER: " + aReply);
        } else {
          System.out.println("SERVER: " + aReply);
        }
      }
    }
  }

}
