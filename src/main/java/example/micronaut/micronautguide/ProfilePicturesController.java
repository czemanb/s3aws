package example.micronaut.micronautguide;

import io.micronaut.http.*;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.multipart.CompletedFileUpload;
import io.micronaut.http.server.types.files.StreamedFile;
import io.micronaut.http.server.util.HttpHostResolver;
import io.micronaut.http.uri.UriBuilder;
import io.micronaut.objectstorage.aws.AwsS3ObjectStorageEntry;
import io.micronaut.objectstorage.aws.AwsS3Operations;
import io.micronaut.objectstorage.request.UploadRequest;
import io.micronaut.objectstorage.response.UploadResponse;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.net.URI;
import java.util.Optional;

@Controller(ProfilePicturesController.PREFIX)
@ExecuteOn(TaskExecutors.IO)
public class ProfilePicturesController implements ProfilePicturesApi {

    static final String PREFIX = "/pictures";

    private final AwsS3Operations objectStorage;
    private final HttpHostResolver httpHostResolver;

    public ProfilePicturesController(AwsS3Operations objectStorage, HttpHostResolver httpHostResolver) {
        this.objectStorage = objectStorage;
        this.httpHostResolver = httpHostResolver;
    }

    @Override
    public HttpResponse<?> upload(CompletedFileUpload fileUpload, String userId, HttpRequest<?> request) {
        String key = buildKey(userId);
        UploadRequest objectStorageUpload = UploadRequest.fromCompletedFileUpload(fileUpload, key);
        UploadResponse<PutObjectResponse> response = objectStorage.upload(objectStorageUpload, builder -> {
            builder.acl(ObjectCannedACL.PUBLIC_READ);
        });

        return HttpResponse
                .created(location(request, userId))
                .header(HttpHeaders.ETAG, response.getETag());
    }

    private static String buildKey(String userId) {
        return userId + ".jpg";
    }

    private URI location(HttpRequest<?> request, String userId) {
        return UriBuilder.of(httpHostResolver.resolve(request))
                .path(PREFIX)
                .path(userId)
                .build();
    }

    @Override
    public Optional<HttpResponse<StreamedFile>> download(String userId) {
        String key = buildKey(userId);
        return objectStorage.retrieve(key)
                .map(ProfilePicturesController::buildStreamedFile);
    }

    private static HttpResponse<StreamedFile> buildStreamedFile(AwsS3ObjectStorageEntry entry) {
        GetObjectResponse nativeEntry = entry.getNativeEntry();
        MediaType mediaType = MediaType.of(nativeEntry.contentType());
        StreamedFile file = new StreamedFile(entry.getInputStream(), mediaType).attach(entry.getKey());
        MutableHttpResponse<Object> httpResponse = HttpResponse.ok()
                .header(HttpHeaders.ETAG, nativeEntry.eTag());
        file.process(httpResponse);
        return httpResponse.body(file);
    }

    @Override
    public void delete(String userId) {
        String key = buildKey(userId);
        objectStorage.delete(key);
    }

}
