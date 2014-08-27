<?php namespace GovTribe\Filebot;

use Guzzle\Batch\BatchBuilder;
use Illuminate\Log\Writer as Log;
use Illuminate\Config\Repository as Config;
use \File;
use Aws\Laravel\AwsFacade as AWS;
use \Exception;

class Filebot
{

    /**
     * AWS s3 instance.
     *
     * @var s3
     */
    protected $s3;

    /**
     * Log writer instance.
     *
     * @var Illuminate\Log\Writer
     */
    protected $log;

    /**
     * Config repository instance.
     *
     * @var Illuminate\Config\Repository
     */
    protected $config;

    /**
     * Temporary directory path.
     *
     * @var string
     */
    protected $workingPath;

    /**
     * Construct an instance of the class.
     *
     * @return void
     */
    public function __construct(Log $log, Config $config)
    {
        $this->log = $log;
        $this->config = $config;
        $this->s3 = AWS::get('s3');

        $this->setupWorkingPath();
    }

    /**
     * Cleanup after the importer goes away.
     *
     * @return void
     */
    public function __destruct()
    {
        $this->deleteWorkingPath();
    }

    /**
     * Setup the working path.
     *
     * @return void
     */
    protected function setupWorkingPath()
    {
        $this->deleteWorkingPath();

        $workingPath  = storage_path();
        $workingPath .= '/filebot/';
        $workingPath .= substr(str_shuffle("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"), 0, 10);
        $this->workingPath = $workingPath;

        File::makeDirectory($this->workingPath, 0770, true, true);
    }

    /**
     * Delete the working path.
     *
     * @return void
     */
    protected function deleteWorkingPath()
    {
        if ($this->workingPath) {
            File::deleteDirectory($this->workingPath, false);
            $this->workingPath = null;
        }
    }

    /**
     * Extract text from a project's attachments.
     *
     * @param string $FBORootProviderGUID
     * @param string $bucketName
     * @param array $files
     * @return void
     */
    public function extractProjectAttachments($FBORootProviderGUID, $bucketName, array $files)
    {
        if (!$FBORootProviderGUID) {
            throw new Exception('Provide a FBORootProviderGUID');
        }
        if (!$bucketName) {
            throw new Exception('Provide a s3 bucket name');
        }
        if (!$files) {
            throw new Exception('Provide a a non-empty list of files');
        }

        // If an existing working path exists, delete it and setup a new one
        if ($this->workingPath) {
            $this->setupWorkingPath();
        }

        // Remap the project's files array to make them easier to deal with, fix URI errors
        $files = $this->remapProjectFiles($files);

        // Filter disallowed file types, locations
        $files = $this->filterFiles($files);

        // Remove duplicate files from the list of files to be processed
        $files = $this->deDupeFileList($FBORootProviderGUID, $bucketName, $files);

        // Download the files
        $files = $this->downloadBatch($files);

        // Convert the files to to text or html
        $files = $this->convertBatch($files);

        // If no files were downloaded, or none were successfully extracted,
        // the list of files may now be empty
        if (!empty($files)) {
            $this->saveToS3Batch($FBORootProviderGUID, $bucketName, $files);

            $this->log->info('Filebot::extractProjectAttachments(): saved batch to s3', [
                'FBORootProviderGUID' => $FBORootProviderGUID,
                'batchSize' => count($files),
            ]);
        }
    }

    /**
     * Get a project's attachments.
     *
     * @param string $FBORootProviderGUID
     * @param string $bucketName
     * @return array
     */
    public function getProjectAttachments($FBORootProviderGUID, $bucketName)
    {
        if (!$FBORootProviderGUID) {
            throw new Exception('Provide a FBORootProviderGUID');
        }
        if (!$bucketName) {
            throw new Exception('Provide a s3 bucket name');
        }

        // No files available
        if (!$this->prefixExists($FBORootProviderGUID, $bucketName)) {
            return [];
        }

        $fileKeys = $this->s3->getIterator('ListObjects', [
            'Bucket' => $bucketName,
            'Prefix' => $FBORootProviderGUID . '/',
        ]);

        $aws = [];

        // Get all of the project's files from s3, and return the file body and name
        foreach ($fileKeys as $fileKey) {
            $file = $this->s3->getObject(array(
                'Bucket' => $bucketName,
                'Delimiter' => '/',
                'Key' => $fileKey['Key'],
            ))->getAll();

            $sizeInBytes = $file['Body']->getSize();
            if ($this->config->get('fboFiles.maxFetchSizeBytes') < $sizeInBytes) {
                $this->log->info('Filebot::getProjectAttachments(): File larger than max fetch size, skipped (' . $this->formatBytes($sizeInBytes) . ' bytes)');
                continue;
            }

            $body = (string) $file['Body'];
            $body = strip_tags($body);
            $body = preg_replace('/\s+/', ' ', $body);
            $body = trim($body);

            if (empty($body)) {
                continue;
            }

            $aws[] = [
                'name' => isset($file['Metadata']['name']) ? $file['Metadata']['name'] : 'Not Available',
                'description' => isset($file['Metadata']['description']) ? $file['Metadata']['description'] : 'Not Available',
                'packageName' => isset($file['Metadata']['packagename']) ? $file['Metadata']['packagename'] : 'Not Available',
                'uri' => $file['Metadata']['uri'],
                'sizeBytes' => $sizeInBytes,
                'file' => $body,
            ];
        }

        $this->log->info('Filebot::getProjectAttachments(): Finished fetching ' . count($aws) . ' files from s3 for FBORootProviderGUID ' . $FBORootProviderGUID);

        // Get unique package names.
        $packageNames = [];
        foreach ($aws as $awsItem) {
            $packageNames[] = $awsItem['packageName'];
        }
        $packageNames = array_keys(array_flip($packageNames));

        // Fillout tempates.
        $toIndex = [];
        foreach ($packageNames as $packageName) {
            $toIndex[$packageName] = [
                'packageDetails' => [
            ],
            'packageName' => $packageName,
            ];
        }

        // Add data from AWS to template.
        foreach ($aws as $awsItem) {
            $toIndex[$awsItem['packageName']]['packageDetails'][] = [
                'fileDescription' => $awsItem['description'],
                'fileName' => $awsItem['name'],
                'fileURI' => $awsItem['uri'],
            ];
        }

        $toIndex = array_values($toIndex);
        return $toIndex;
    }

    /**
     * Save a batch of files to s3.
     *
     * @param string $FBORootProviderGUID
     * @param string $bucketName
     * @param array $files
     * @return void
     */
    protected function saveToS3Batch($FBORootProviderGUID, $bucketName, array $files = array())
    {
        try {
            $batch = BatchBuilder::factory()
                ->transferCommands(10)
                ->autoFlushAt(5)
                ->build();

            foreach ($files as $item) {
                $key = $FBORootProviderGUID . '/' . $item['saveAs'];
                $opts = [
                    'Bucket' => $bucketName,
                    'Key' => $key,
                    'Body' => $item['convertOK'] ? $item['extractedHTML'] : $item['md5'],
                    'ContentType' => $item['ContentType'],
                    'Metadata' => [
                        'FBORootProviderGUID' => $FBORootProviderGUID,
                        'name' => (string) $item['name'],
                        'uri' => $item['uri'],
                        'description' => (string) $item['description'],
                        'packageName' => (string) $item['packageName'],
                        'md5' => $item['md5'],
                        'convertOK' => $item['convertOK'] ? 'yes' : 'no',
                    ]
                ];

                $batch->add($this->s3->getCommand('PutObject', $opts));
            }

            $batch->flush();
        } catch (Exception $e) {
            $this->log->error('Filebot::saveToS3Batch(): ' . $e->getMessage(), $opts['Metadata']);
        }
    }

    /**
     * Convert a batch of files to html.
     *
     * @param array $files
     * @return array
     */
    protected function convertBatch(array $files)
    {
        foreach ($files as &$item) {
            //$this->log->info('Filebot::convertBatch: Converting file "' . $item['name'] . '"');

            // Try to convert the file using Tika
            $result = $this->convertToHTML($this->workingPath . '/' . $item['saveAs'], $item);

            $item['convertOK'] = false;

            // Result is null
            if (!$result) {
                //
            } elseif (mb_strlen($result) < 200) {
                // Result too short
            } else {
                $item['convertOK'] = true;
                $item['extractedHTML'] = preg_replace('@\x{FFFD}@u', '_', $result);
            }

            $item['ContentType'] = 'text/html';
        }

        return $files;
    }

    /**
     * Convert a file to HTML.
     *
     * @param string $path
     * @return string
     */
    public function convertToHTML($path)
    {
        $result = null;
        $fh = fopen($path, 'r');

        try {
            $ch = curl_init('http://localhost:9998/tika');
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
            curl_setopt($ch, CURLOPT_PUT, 1);
            curl_setopt($ch, CURLOPT_BINARYTRANSFER, true);
            curl_setopt($ch, CURLOPT_HTTPHEADER, ['Accept: text/html']);
            curl_setopt($ch, CURLOPT_TIMEOUT, 15);
            curl_setopt($ch, CURLOPT_INFILE, $fh);
            curl_setopt($ch, CURLOPT_INFILESIZE, File::size($path));
            $response = curl_exec($ch);
            curl_close($ch);

            $result = $response;
        } catch (Exception $e) {
            $this->log->error('Filebot::convertToHTML(): ' . $e->getMessage());
            $result = null;
        }

        fclose($fh);
        return $result;
    }

    /**
     * Download a batch of files.
     *
     * @param array $files
     * @return array
     */
    protected function downloadBatch(array $files)
    {
        foreach ($files as &$item) {
            $tempSavePath = $this->workingPath . '/' . rand(0, 100000);
            $result = $this->downloadOne($item['uri'], $tempSavePath);

            // Download failed...
            if (!$result) {
                $item = false;
            } else {
                // Rename the file to its md5 has value
                $savePath = $this->workingPath . '/' . $item['saveAs'];
                File::move($tempSavePath, $savePath);
                $item['md5'] = md5_file($savePath);
            }
        }

        // Remove failed downloads from the list of files to save
        $files = array_filter($files);

        return $files;
    }

    /**
     * Download a single file.
     *
     * @param string $fileURL
     * @param string $savePath
     * @return bool
     */
    protected function downloadOne($fileURL, $savePath)
    {
        $result = false;

        try {
            switch (parse_url($fileURL, PHP_URL_SCHEME))
            {
                case 'http':
                case 'https':

                        //$this->log->info('Filebot::downloadOne: Downloading file: "' . $fileURL . '"');

                        $client = new \Guzzle\Http\Client();

                        $request = $client->get($fileURL)->setResponseBody($savePath);
                        $request->getCurlOptions()->set(CURLOPT_SSL_VERIFYHOST, false);
                        $request->getCurlOptions()->set(CURLOPT_SSL_VERIFYPEER, false);
                        $request->getCurlOptions()->set(CURLOPT_CONNECTTIMEOUT, 2);
                        $request->getCurlOptions()->set(CURLOPT_TIMEOUT, 30);

                        $response = $request->send();

                    if ($response->getStatusCode() === 200) {
                            $result = true;
                    }

                    break;
            }
        } catch (Exception $e) {
            $this->log->info('Filebot::downloadOne(): ' . $e->getMessage());
        }

        return $result;
    }

    /**
     * Remove files we've already scraped from the list of those to process.
     *
     * @param string $FBORootProviderGUID
     * @param string $bucketName
     * @param array $files
     * @return array
     */
    protected function deDupeFileList($FBORootProviderGUID, $bucketName, array $files)
    {
        // Remove files we've already processed from the list of those to process
        foreach ($files as &$item) {
            $fileKey = md5($item['uri']);

            if ($this->s3->doesObjectExist($bucketName, $FBORootProviderGUID . '/' . $fileKey)) {
                //$this->log->info('Filebot::deDupeFileList(): Skipped existing file "' . $item['name'] . '"');
                $item = false;
            }
        }

        return array_filter($files);
    }

    /**
     * Check if a file already exists in a bucket.
     *
     * @param string $prefix
     * @param string $fileName
     * @param string $bucketName
     * @return bool
     */
    protected function fileExists($prefix, $fileName, $bucketName)
    {
        $result = $this->s3->listObjects([
            'Bucket' => $bucketName,
            'Prefix' => $prefix . '/',
            'MaxKeys' => 1
        ]);

        if (!$result['Contents'] && !$result['CommonPrefixes']) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Check if a prefix already exists in a bucket.
     *
     * @param string $prefix
     * @param string $bucketName
     * @return bool
     */
    protected function prefixExists($prefix, $bucketName)
    {
        $result = $this->s3->listObjects([
            'Bucket' => $bucketName,
            'Prefix' => $prefix . '/',
            'MaxKeys' => 1
        ]);

        if (!$result['Contents'] && !$result['CommonPrefixes']) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Delete a group of files by their prefix.
     *
     * @param string $prefix
     * @param string $bucketName
     * @return bool
     */
    protected function deletePrefix($prefix, $bucketName)
    {
        $fileKeys = $this->s3->getIterator('ListObjects', [
            'Bucket' => $bucketName,
            'Prefix' => $prefix . '/',
        ]);

        foreach ($fileKeys as $fileKey) {
            $this->s3->deleteObject(array(
                'Bucket' => $bucketName,
                'Delimiter' => '/',
                'Key' => $fileKey['Key'],
            ));
        }
    }

    /**
     * Remap an array of files to make them less error prone to download.
     *
     * @param array $value
     * @return array
     */
    protected function remapProjectFiles(array $value)
    {
        $output = [];

        foreach ($value as $packageGroup) {
            if (isset($packageGroup['packageSecure']) && $packageGroup['packageSecure'] === true) {
                continue;
            }

            $packageName = $packageGroup['packageName'];
            $packageDetails = $packageGroup;
            unset($packageDetails['packageName'], $packageDetails['packageType'], $packageDetails['packageSecure']);

            foreach ($packageDetails as $item) {
                if (!isset($item['uri'])) {
                    continue;
                }

                // Fix an issue in file URLs that contain an ftp address
                $item['uri'] = str_replace('https://www.fbo.govFTP://', 'ftp://', $item['uri']);

                // Add $packageName to the item
                $item['packageName'] = $packageName;

                // Add a saveAs value to the item
                $item['saveAs'] = md5($item['uri']);

                // Provide default values for name, description and package name.
                if (!isset($item['name'])) {
                    $item['name'] = 'Not Available';
                }
                if (!isset($item['description'])) {
                    $item['description'] = 'Not Available';
                }
                if (!isset($item['packageName'])) {
                    $item['description'] = 'Not Available';
                }

                // All s3 metadata must be saved as ASCII
                foreach ($item as $key => &$value) {
                    if ($key == 'uri' || $key == 'saveAs') {
                        continue;
                    }

                    $value = $this->cleanDirtyHTML($value);
                    $value = @iconv('UTF-8', 'ASCII//IGNORE', $value);
                }

                $output[] = $item;
            }
        }
        return $output;
    }

    /**
     * Filter disallowed file types, locations.
     *
     * @param array $files
     * @return array
     */
    protected function filterFiles(array $files)
    {
        $stop = [
            '\.mp4',
            '\.m4v',
            '\.webm',
            '\.ogv',
            '\.wmv',
            '\.flv',
            '\.zip',
        ];

        $stop = implode('|', $stop);

        foreach ($files as &$item) {
            $needle = $item['uri'] . $item['name'];

            $matches = preg_match_all('#' . $stop . '#', $needle);

            if ($matches) {
                $item = false;
            }
        }

        $files = array_filter($files);
        return $files;
    }

    /**
     * Clean dirty HTML.
     *
     * @param  string $string
     * @param  string $keepTags
     * @return string
     */
    public function cleanDirtyHTML($string, $keepTags = '<p><br>')
    {
        mb_regex_encoding('UTF-8');

        // Remove all html tags except those specified
        $string = strip_tags($string, $keepTags);

        // Remove all class attributes from tags
        $string = preg_replace('/class=".*?"/', '', $string);

        // Remove all style attributes from tags
        $string = preg_replace('/style=".*?"/', '', $string);

        // Normalize newlines
        $string = preg_replace('/(\r\n|\r|\n)+/', "\n", $string);

        // Replace whitespace characters with a single space
        $string = preg_replace('/\s+/', ' ', $string);

        // Trim trailing, leading spaces
        $string = trim($string);

        return $string;
    }
}
