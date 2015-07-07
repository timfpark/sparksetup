<# 
.SYNOPSIS 
  Install Spark to HDInsight cluster.
   
.DESCRIPTION 
  This installs Apache Spark on HDInsight cluster and it runs on YARN. 
 
.EXAMPLE 
  .\spark-installer-v03.ps1 -SparkBinaryZipLocation https://msibuilds.blob.core.windows.net/preview/spark-preview/spark-1.2.1.2.2.2.2-0001.winpkg.zip -SparkRootName spark-1.2.1
#> 
 
param ( 
    # The binary location for Spark in zip format. 
    [Parameter()] 
    [String]$SparkBinaryZipLocation, 
 
    # The name of the folder for Spark root.
    [Parameter()] 
    [String]$SparkRootName) 

    
# Download config action module from a well-known directory.
$CONFIGACTIONURI = "https://hdiconfigactions.blob.core.windows.net/configactionmodulev03/HDInsightUtilities-v03.psm1";
$CONFIGACTIONMODULE = "C:\HDInsightUtilities.psm1";
$webclient = New-Object System.Net.WebClient;
$webclient.DownloadFile($CONFIGACTIONURI, $CONFIGACTIONMODULE);

# (TIP) Import config action helper method module to make writing config action easy.
if (Test-Path ($CONFIGACTIONMODULE))
{ 
    Import-Module $CONFIGACTIONMODULE;
} 
else
{
    Write-Output "Failed to load HDInsightUtilities module, exiting ...";
    exit;
}

#if (Test-IsHDIDataNode)
#{
#    Write-HDILog "Spark on YARN only need to be installed on headnode, exiting ...";
#    exit;
#}

$customizeComputePath = $false;

if (((Get-HDIHadoopVersion).StartsWith("hadoop-2.6"))) 
{
    $hdiVersion = "3.2";

    if (Test-Path "$env:HADOOP_HOME\share\hadoop\common\hadoop-azure-*.jar")
    {
        $customizeComputePath = $true;
    }    
} 
elseif (((Get-HDIHadoopVersion).StartsWith("hadoop-2.4"))) 
{
    $hdiVersion = "3.1";
}
else 
{
    Write-HDILog "Spark on HDInsight only supports 2.6 and 2.4 Hadoop cluster at the moment.";
    exit;
} 

# Use default parameters in case they are not specified.
if (!$SparkBinaryZipLocation) 
{ 
    if ($hdiVersion.Equals("3.1"))
    {
        $SparkBinaryZipLocation = "https://hdiconfigactions.blob.core.windows.net/sparkconfigactionv01/spark-1.0.2.zip"; 
    }
    else
    {
        if ($customizeComputePath)
        {
            $SparkBinaryZipLocation = "https://msibuilds.blob.core.windows.net/preview/spark-preview/spark-1.2.1.2.2.2.2-0001.winpkg.zip"; 
            $SparkComputePathLocation = "https://hdiconfigactions.blob.core.windows.net/sparkconfigactionv03/compute-classpath-v03.cmd";
        }
        else
        {
            $SparkBinaryZipLocation = "https://msibuilds.blob.core.windows.net/preview/spark-preview/spark-1.2.0.2.2.1.0-2340.winpkg.zip"; 
        }

        $HiveSiteXmlLocation = "https://hdiconfigactions.blob.core.windows.net/sparkconfigactionv03/hive-site-v03.xml";
    }
}

if (!$SparkRootName) 
{ 
    if ($hdiVersion.Equals("3.1"))
    {
        $SparkRootName = "spark-1.0.2"; 
    }
    else
    {
        $sparkfilename = $SparkBinaryZipLocation.Substring($SparkBinaryZipLocation.LastIndexOf("/") + 1);
        $sparktmpname = $sparkfilename.Replace(".winpkg.zip", "");
        $SparkRootName = $sparkfilename.Substring(0, 11); 
    }
}    

# (TIP) Write-HDILog is the way to write to STDOUT and STDERR in HDInsight config action script.
Write-HDILog "Starting Spark installation at: $(Get-Date)";

# Define input to the Spark installation script.
$sparkname = $SparkRootName;
$src = $SparkBinaryZipLocation;
$sparkinstallationdir=(Get-Item "$env:HADOOP_HOME").parent.FullName;

# (TIP) Test whether the destination file already exists and this makes the script idempotent so it functions properly upon reboot and reimage.
if (Test-Path ($sparkinstallationdir + '\' + $sparkname)) 
{
    Write-HDILog "Destination: $sparkinstallationdir\$sparkname already exists, exiting ...";
    exit;
}

# Create destination directory if not exists.
New-Item -ItemType Directory -Force -Path $sparkinstallationdir;

# Download the zip file into local file system.
# (TIP) It is always good to download to user temporary location.
$intermediate = $env:temp + '\' + $sparkname + [guid]::NewGuid() + '.zip';
Save-HDIFile -SrcUri $src -DestFile $intermediate;

[Environment]::SetEnvironmentVariable('SPARK_HOME', $sparkinstallationdir + '\' + $sparkname, 'Machine');

[Environment]::SetEnvironmentVariable(‘PYTHONPATH’, $sparkinstallationdir + '\' + $sparkname + ‘\python\;’ + $Env:PYTHONPATH, 'Machine');

# Unzip the file into final destination.
if ($hdiVersion.Equals("3.1"))
{
    Expand-HDIZippedFile -ZippedFile $intermediate -UnzipFolder $sparkinstallationdir;
    $output = Invoke-HDICmdScript -CmdToExecute "COPY /Y %HIVE_HOME%\conf\hive-site.xml $sparkinstallationdir\$sparkname\conf";
    Write-HDILog $output;
} 
else 
{
    $tmpdir = $env:temp + '\' + $sparktmpname + [guid]::NewGuid();
    New-Item -ItemType Directory -Force -Path $tmpdir;
    Expand-HDIZippedFile -ZippedFile $intermediate -UnzipFolder "$tmpdir";
    Expand-HDIZippedFile -ZippedFile "$tmpdir\resources\$sparktmpname.zip" -UnzipFolder $sparkinstallationdir;
    Remove-Item -Path "$tmpdir" -Force -Recurse -ErrorAction SilentlyContinue;
    Move-Item "$sparkinstallationdir\$sparktmpname" "$sparkinstallationdir\$SparkRootName";
    
    # Copy hive-site.xml to Spark to enable Spark SQL.
    if ($HiveSiteXmlLocation) 
    {
        Save-HDIFile -SrcUri $HiveSiteXmlLocation -DestFile "$sparkinstallationdir\$SparkRootName\conf\hive-site.xml" -ForceOverwrite $true;
    }

    # Copy compute-classpath.cmd to Spark if needed.
    if ($SparkComputePathLocation) 
    {
        Save-HDIFile -SrcUri $SparkComputePathLocation -DestFile "$sparkinstallationdir\$SparkRootName\bin\compute-classpath.cmd" -ForceOverwrite $true; 
    }
}

# Remove the intermediate file we created.
# (TIP) Please clean up temporary files when no longer needed.
Remove-Item $intermediate;

# Setup environment variables as well as history server directory to keep Spark job history.
# (TIP) This is the way to capture STDOUT and STDERR for processes printing to console as only Write-HDILog is the way to print to STDOUT and STDERR.
$output = Invoke-HDICmdScript -CmdToExecute "%HADOOP_HOME%\bin\hadoop fs -mkdir -p /spark-history";
Write-HDILog $output;
$output = Invoke-HDICmdScript -CmdToExecute "%HADOOP_HOME%\bin\hadoop fs -chmod -R 777 /spark-history";
Write-HDILog $output;

Write-HDILog "Done with Spark installation at: $(Get-Date)";

Write-HDILog "Installed Spark at: $sparkinstallationdir\$sparkname";
