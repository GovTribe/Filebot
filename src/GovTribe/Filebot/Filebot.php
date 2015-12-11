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

        if (strpos($bucketName, '/') !== false)
        {
            $prefix = explode('/', $bucketName)[1] . '/' .$FBORootProviderGUID . '/';
            $bucketName = explode('/', $bucketName)[0];
        }
        else $prefix = $FBORootProviderGUID . '/';

        $fileKeys = $this->s3->getIterator('ListObjects', [
            'Bucket' => $bucketName,
            'Prefix' => $prefix,
        ]);

        $aws = [];

        // Get all of the project's files from s3, and return the file body and name
        foreach ($fileKeys as $fileKey) {
            $file = $this->s3->getObject(array(
                'Bucket' => $bucketName,
                'Delimiter' => '/',
                'Key' => $fileKey['Key'],
            ))->getAll();
            
            if (isset($file['Metadata']['binary'])) continue;

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

            if (isset($file['Metadata']['binary'])) continue;

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
                'file' => $awsItem['file'],
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
        if (strpos($bucketName, '/') !== false)
        {
            $prefix = explode('/', $bucketName)[1] . '/' .$prefix;
            $bucketName = explode('/', $bucketName)[0];
        }

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

    /**
     * Check if an array is multidimensional.
     *
     * @param array $array
     *
     * @return bool
     */
    public function isArrayMultiDim(array $array)
    {
        if (count($array) == count($array, COUNT_RECURSIVE)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Parse an html string into an XPath object.
     *
     * @param string $html
     *
     * @return DOMXpath
     */
    protected function getXPathFromHTML($html)
    {
        if (empty($html)) {
            throw new \InvalidArgumentException;
        }

        // Set DOM
        libxml_use_internal_errors(true);
        $DOM = new DOMDocument('1.0', 'UTF-8');
        $DOM->recover = true;
        $DOM->loadHTML($html);
        libxml_use_internal_errors(false);

        // Set XPath
        $XPath = new DOMXpath($DOM);

        if (!$XPath) {
            throw new \ErrorException('Could not build XPath from html: ' . $html);
        } else {
            return $XPath;
        }
    }

    /**
     * Extract a single item from a XPath object.
     *
     * @param DOMXpath
     * @param string $selector XPath query
     * @param bool $contextNode Provide a context node
     * @param bool $html Get the node's html
     *
     * @return string
     */
    protected function getSingleDOMItem(DOMXpath $XPath, $selector, $contextNode = false, $html = false)
    {
        $nodeList = $contextNode ? $XPath->query($selector, $contextNode) : $XPath->query($selector);

        if (is_object($nodeList) && $nodeList->length != 0) {
            if ($html) {
                $item = $nodeList->item(0);

                $temp = new DOMDocument('1.0', 'UTF-8');
                $cloned = $item->cloneNode(true);
                $temp->appendChild($temp->importNode($cloned, true));

                return $temp->saveHTML();

            } else {
                return trim($nodeList->item(0)->nodeValue);
            }
        } else {
            return null;
        }
    }

    /**
     * Format a number of bytes as kilobytes, gigabytes, etc.
     *
     * @param  int $bytes
     * @param  int $precision
     * @return int
     */
    public function formatBytes($bytes, $precision = 2)
    {
        $units = array('B', 'KB', 'MB', 'GB', 'TB');

        $bytes = max($bytes, 0);
        $pow = floor(($bytes ? log($bytes) : 0) / log(1024));
        $pow = min($pow, count($units) - 1);

        // Uncomment one of the following alternatives
        //$bytes /= pow(1024, $pow);
        $bytes /= (1 << (10 * $pow));

        return round($bytes, $precision) . ' ' . $units[$pow];
    }

    /**
     * Add the ordinal suffix to numbers.
     *
     * @param  int $number
     * @return string
     */
    public function addOrdinalNumberSuffix($number)
    {
        if (!in_array(($number % 100), array(11,12,13))) {
            switch ($number % 10)
            {
                // Handle 1st, 2nd, 3rd
                case 1:
                    return $number.'st';
                case 2:
                    return $number.'nd';
                case 3:
                    return $number.'rd';
            }
        }

        return $number.'th';
    }
}
