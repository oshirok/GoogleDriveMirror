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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;


public class GDTest {

	private static String CLIENT_ID = "573610413961-6a6imko3r9h83laqo4i1i9c50q878hja.apps.googleusercontent.com";
	private static String CLIENT_SECRET = "nosQ6RfgUAnHchsyAOemolkJ";

	private static String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";

	public static void main(String[] args) throws IOException {
		HttpTransport httpTransport = new NetHttpTransport();
		JsonFactory jsonFactory = new JacksonFactory();

		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				httpTransport, jsonFactory, CLIENT_ID, CLIENT_SECRET, Arrays.asList(DriveScopes.DRIVE))
		.setAccessType("online")
		.setApprovalPrompt("auto").build();

		String url = flow.newAuthorizationUrl().setRedirectUri(REDIRECT_URI).build();
		System.out.println("Please open the following URL in your browser then type the authorization code:");
		System.out.println("  " + url);
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String code = br.readLine();

		GoogleTokenResponse response = flow.newTokenRequest(code).setRedirectUri(REDIRECT_URI).execute();
		GoogleCredential credential = new GoogleCredential().setFromTokenResponse(response);

		//Create a new authorized API client
		Drive service = new Drive.Builder(httpTransport, jsonFactory, credential).build();
		Set<String> tmp1 = listLocalFiles();
		Set<String> tmp2 = retrieveAllGDFiles(service);
		tmp2.retainAll(tmp1);
		tmp1.removeAll(tmp2);
		String[] files = tmp1.toArray(new String[tmp1.size()]);
		String[] files1 = new String[files.length / 2];
		String[] files2 = new String[files.length - files.length / 2];
		System.arraycopy(files, 0, files1, 0, files1.length);
		System.arraycopy(files, files1.length, files2, 0, files2.length);
		
		Upload.uploadArray(files, service);
		/*java.util.Iterator<String> itty = tmp1.iterator();
		while (itty.hasNext()) {
			String filename = itty.next();
			File file = insertFile(service, filename, "",
					"0B5c7iu0bimJXfkZZVlR2eHFQd0paY2lHYWVQM3FfeWJ0WjYya1NkMlJtWjJER05OWVd1UkE", java.nio.file.Files.probeContentType(Paths.get("150D5200/" + filename)), "150D5200/" + filename);
			System.out.println(file.getTitle());
		}*/
		System.out.println("DONE!");
	}

	/**
	 * Retrieve a list of File resources.
	 *
	 * @param service Drive API service instance.
	 * @return List of File resources.
	 */
	private static Set<String> retrieveAllGDFiles(Drive service) throws IOException {
		List<File> result = new ArrayList<File>();
		Files.List request = service.files().list().setQ("'0B5c7iu0bimJXfkZZVlR2eHFQd0paY2lHYWVQM3FfeWJ0WjYya1NkMlJtWjJER05OWVd1UkE' in parents");
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

	private static Set<String> listLocalFiles() {
		java.io.File folder = new java.io.File("150D5200");
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
	
	    Upload(String[] arr, int lo, int hi, Drive serv) {
	        array = arr;
	        low   = lo;
	        high  = hi;
	        service = serv;
	    }
	
	    protected Long compute() {
	        if(high - low <= SEQUENTIAL_THRESHOLD) {
	        	for(int i = low; i < high; i++) {
					String filename = array[i];
					File file;
					try {
						file = insertFile(service, filename, "",
								"0B5c7iu0bimJXfkZZVlR2eHFQd0paY2lHYWVQM3FfeWJ0WjYya1NkMlJtWjJER05OWVd1UkE", java.nio.file.Files.probeContentType(Paths.get("150D5200/" + filename)), "150D5200/" + filename);
						System.out.println(file.getTitle());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	         } else {
	            int mid = low + (high - low) / 2;
	            Upload left  = new Upload(array, low, mid, service);
	            Upload right = new Upload(array, mid, high, service);
	            left.fork();
	            right.compute();
	            left.join();
	         }
            return (long) 0;
	     }
	
	     static void uploadArray(String[] array, Drive serv) {
	         Globals2.fjPool.invoke(new Upload(array,0,array.length, serv));
	     }
	}

}
