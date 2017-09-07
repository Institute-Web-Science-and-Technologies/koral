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

import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.SaltedPasswordEncryptor;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * Starts an FTP server for uploading and downloading files.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class FTPServer implements Closeable {

  final static String DEFAULT_USER_NAME = "user";

  final static String DEFAULT_PASSWORD = "user";

  private File ftpFolder;

  private FtpServer server;

  public void start(String ipAddress, String port, File workingDir, int maxNumberOfLogins) {
    start(ipAddress, port, workingDir, FTPServer.DEFAULT_USER_NAME, FTPServer.DEFAULT_PASSWORD,
            maxNumberOfLogins);
  }

  public void start(String ipAddress, String port, File workingDir, String username,
          String password, int maxNumberOfLogins) {
    ftpFolder = new File(workingDir.getAbsolutePath() + File.separator + "ftp");
    if (!ftpFolder.exists()) {
      ftpFolder.mkdirs();
    }
    try {
      // configure network
      ListenerFactory factory = new ListenerFactory();
      factory.setServerAddress(ipAddress);
      factory.setPort(Integer.parseInt(port));

      // configure user
      PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
      userManagerFactory.setPasswordEncryptor(new SaltedPasswordEncryptor());
      File upf = new File(ftpFolder.getAbsolutePath() + File.separator + "ftpUserFile");
      if (!upf.exists()) {
        upf.createNewFile();
      }
      userManagerFactory.setFile(upf);
      UserManager userManager = userManagerFactory.createUserManager();
      BaseUser user = new BaseUser();
      user.setName(username);
      user.setPassword(password);
      user.setHomeDirectory(workingDir.getAbsolutePath());
      List<Authority> authorities = new ArrayList<>();
      authorities.add(new WritePermission());
      user.setAuthorities(authorities);
      userManager.save(user);

      ConnectionConfigFactory connectionConfig = new ConnectionConfigFactory();
      connectionConfig.setMaxLogins(maxNumberOfLogins);
      connectionConfig.setAnonymousLoginEnabled(false);

      // crate and start server
      FtpServerFactory serverFactory = new FtpServerFactory();
      serverFactory.addListener("default", factory.createListener());
      serverFactory.setUserManager(userManager);
      serverFactory.setConnectionConfig(connectionConfig.createConnectionConfig());
      server = serverFactory.createServer();
      server.start();
    } catch (IOException | FtpException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (server != null) {
      server.stop();
    }
    deleteFolder(ftpFolder);
  }

  private void deleteFolder(File folder) {
    if ((folder != null) && folder.exists()) {
      for (File file : folder.listFiles()) {
        file.delete();
      }
      folder.delete();
    }
  }

}
