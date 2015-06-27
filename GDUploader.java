import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Files;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.ParentReference;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;


public class GDUploader {

	private static String CLIENT_ID = "573610413961-6a6imko3r9h83laqo4i1i9c50q878hja.apps.googleusercontent.com";
	private static String CLIENT_SECRET = "nosQ6RfgUAnHchsyAOemolkJ";
	private static String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";

	public static void main(String[] args) throws IOException {
		HttpTransport httpTransport = new NetHttpTransport();
		JsonFactory jsonFactory = new JacksonFactory();

		// For authorizing with Google Account
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				httpTransport, jsonFactory, CLIENT_ID, CLIENT_SECRET, Arrays.asList(DriveScopes.DRIVE))
		.setAccessType("online")
		.setApprovalPrompt("auto").build();

		String url = flow.newAuthorizationUrl().setRedirectUri(REDIRECT_URI).build();
		System.out.println("Please open the following URL in your browser then type the authorization code:");
		System.out.println("  " + url);
		Scanner scanner = new Scanner(System.in);
		String code = scanner.nextLine();

		GoogleTokenResponse response = flow.newTokenRequest(code).setRedirectUri(REDIRECT_URI).execute();
		GoogleCredential credential = new GoogleCredential().setFromTokenResponse(response);

		//Create a new authorized API client
		Drive service = new Drive.Builder(httpTransport, jsonFactory, credential).build();
		
		// Ask the user for the local folder source name
		System.out.println("Enter: the local source folder name:");
		String local_folder_name = scanner.nextLine();
		
		// Verify that the file exists, if not prompt again
		java.io.File f = new java.io.File(local_folder_name);
		while(!f.exists() || !f.isDirectory()) {
			System.out.println("Invalid File. Enter: the local source folder name:");
			local_folder_name = scanner.nextLine();
		}
		
		// Now Ask for and verify that the Google Drive target folder exists
		String folder_id = null;
		boolean folder_found = false;
		while(!folder_found) {
			System.out.println("Enter: the Google Drive target folder name:");
			String name = scanner.nextLine();
			try {
				folder_id = getFolderByName(name, service);
				folder_found = true;
			} catch (IOException e) {
				System.out.println("Google Drive target name not found try again");
			} catch (IllegalArgumentException e) {
				System.out.println("Google Drive target name not found try again");
			}
			// Prompt for folder name
		}
		
		// Set operations to get files that dont already exist
		Set<String> tmp1 = listLocalFiles(local_folder_name);
		Set<String> tmp2 = retrieveAllGDFiles(service, folder_id);
		tmp2.retainAll(tmp1);
		tmp1.removeAll(tmp2);
		
		// Split the files array recursively for Parallel Processing
		String[] files = tmp1.toArray(new String[tmp1.size()]);
		String[] files1 = new String[files.length / 2];
		String[] files2 = new String[files.length - files.length / 2];
		System.arraycopy(files, 0, files1, 0, files1.length);
		System.arraycopy(files, files1.length, files2, 0, files2.length);
		
		Upload.uploadArray(files, service, folder_id, local_folder_name);
		System.out.println("DONE!");
	}

	/**
	 * Retrieve a list of File resources.
	 *
	 * @param service Drive API service instance.
	 * @return List of File resources.
	 */
	private static Set<String> retrieveAllGDFiles(Drive service, String folderid) throws IOException {
		List<File> result = new ArrayList<File>();
		Files.List request = service.files().list().setQ("'" + folderid + "'" + " in parents");
		Set<String> setty = new HashSet<String>();

		System.out.println("Loading files from Google Drive...");
		do {
			try {
				FileList files = request.execute();
				result.addAll(files.getItems());
				request.setPageToken(files.getNextPageToken());
			} catch (IOException e) {
				System.out.println("An error occurred: " + e);
				request.setPageToken(null);
			}
		} while (request.getPageToken() != null &&
				request.getPageToken().length() > 0);

		for(File file: result) {
			setty.add(file.getTitle());
			// System.out.println(file.getTitle());
		}
		return setty;
	}

	private static Set<String> listLocalFiles(String foldername) {
		java.io.File folder = new java.io.File(foldername);
		java.io.File[] listOfFiles = folder.listFiles();
		Set<String> setty = new HashSet<String>();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				setty.add(listOfFiles[i].getName());
			} else if (listOfFiles[i].isDirectory()) {
				setty.add(listOfFiles[i].getName());
			}
		}
		return setty;
	}
	
	private static Map<String, File> getFolders(Drive service) throws IOException {
		List<File> result = new ArrayList<File>();
		Files.List request = service.files().list().setQ("mimeType = 'application/vnd.google-apps.folder'");
		Map<String, File> setty = new HashMap<String, File>();
		do {
			try {
				FileList files = request.execute();
				result.addAll(files.getItems());
				request.setPageToken(files.getNextPageToken());
			} catch (IOException e) {
				System.out.println("An error occurred: " + e);
				request.setPageToken(null);
			}
		} while (request.getPageToken() != null &&
				request.getPageToken().length() > 0);

		for(File file: result) {
			setty.put(file.getTitle(), file);
			// System.out.println(file.getTitle());
		}
		return setty;
	}
	
	private static String getFolderByName(String name, Drive service) throws IOException {
		Map<String, File> mappy = getFolders(service);
		if (mappy.get(name) == null) throw new IllegalArgumentException("Enter a valid folder name");
		return mappy.get(name).getId();
	}

	/**
	 * Insert new file.
	 *
	 * @param service Drive API service instance.
	 * @param title Title of the file to insert, including the extension.
	 * @param description Description of the file to insert.
	 * @param parentId Optional parent folder's ID.
	 * @param mimeType MIME type of the file to insert.
	 * @param filename Filename of the file to insert.
	 * @return Inserted file metadata if successful, {@code null} otherwise.
	 */
	private static File insertFile(Drive service, String title, String description,
			String parentId, String mimeType, String filename) {
		// File's metadata.
		File body = new File();
		body.setTitle(title);
		body.setDescription(description);
		body.setMimeType(mimeType);

		// Set the parent folder.
		if (parentId != null && parentId.length() > 0) {
			body.setParents(
					Arrays.asList(new ParentReference().setId(parentId)));
		}

		// File's content.
		java.io.File fileContent = new java.io.File(filename);
		FileContent mediaContent = new FileContent(mimeType, fileContent);
		try {
			File file = service.files().insert(body, mediaContent).execute();

			// Uncomment the following line to print the File ID.
			// System.out.println("File ID: " + file.getId());

			return file;
		} catch (IOException e) {
			System.out.println("An error occured: " + e);
			return null;
		}
	}

	static class Globals2 {
	    final static ForkJoinPool fjPool = new ForkJoinPool();
	}
	
	static class Upload extends RecursiveTask<Long> {
	    static final int SEQUENTIAL_THRESHOLD = 200;
	
	    int low;
	    int high;
	    String[] array;
	    Drive service;
	    String folderid;
	    String foldername;
	
	    Upload(String[] arr, int lo, int hi, Drive serv, String fo_id, String local_fo_name) {
	        array = arr;
	        low   = lo;
	        high  = hi;
	        service = serv;
	        folderid = fo_id;
	        foldername = local_fo_name;
	    }
	
	    protected Long compute() {
	        if(high - low <= SEQUENTIAL_THRESHOLD) {
	        	for(int i = low; i < high; i++) {
					String filename = array[i];
					File file;
					try {
						file = insertFile(service, filename, "",
								folderid, java.nio.file.Files.probeContentType(Paths.get(foldername + "/" + filename)), foldername + "/" + filename);
						System.out.println(file.getTitle());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	         } else {
	            int mid = low + (high - low) / 2;
	            Upload left  = new Upload(array, low, mid, service, folderid, foldername);
	            Upload right = new Upload(array, mid, high, service, folderid, foldername);
	            left.fork();
	            right.compute();
	            left.join();
	         }
            return (long) 0;
	     }
	
	     static void uploadArray(String[] array, Drive serv, String folderid, String foldername) {
	         Globals2.fjPool.invoke(new Upload(array,0,array.length, serv, folderid, foldername));
	     }
	}

}
