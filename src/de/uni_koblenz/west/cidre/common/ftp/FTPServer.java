package de.uni_koblenz.west.cidre.common.ftp;

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

  public void start(String ipAddress, String port, File workingDir) {
    start(ipAddress, port, workingDir, FTPServer.DEFAULT_USER_NAME, FTPServer.DEFAULT_PASSWORD);
  }

  public void start(String ipAddress, String port, File workingDir, String username,
          String password) {
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

      // crate and start server
      FtpServerFactory serverFactory = new FtpServerFactory();
      serverFactory.addListener("default", factory.createListener());
      serverFactory.setUserManager(userManager);
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
