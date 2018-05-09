function convertGallant(directory, dryRun)
if ~exist('dryRun', 'var')
    dryRun = false;
end
if ~exist('directory', 'var')
    directory = 'V1Data/NatRev';
end
addpath(genpath([directory, '/../functions']));
scriptDir = fileparts(mfilename('fullpath'));
addpath(genpath([scriptDir, '/lib']));
files = glob([directory, '/*/*summary_file.mat']);
for i = 1:length(files)
    file = files{i};
    [subDir, ~, ~] = fileparts(file);
    summaries = load(file);
    summaries = summaries.celldata;
    for summary = summaries'
        %% read and align stimuli and responses
        stimuliFile = [subDir, '/', summary.stimfile];
        stimuli = loadimfile(stimuliFile);
        stimuli = uint8(stimuli);
        responseFile = [subDir, '/', summary.respfile];
        response = respload(responseFile);
        response = response(:, 1);
        if length(response) > size(stimuli, 3)
            fprintf('Shortening length %d response to %d\n', ...
                length(response), size(stimuli, 3));
            response = response(1:size(stimuli, 3));
        elseif length(response) < size(stimuli, 3)
            fprintf('Shortening length %d stimuli to %d\n', ...
                size(stimuli, 3), length(response));
            stimuli = stimuli(:, :, 1:length(response));
        end
        assert(length(response) == size(stimuli, 3));
        keptFixation = ~ismember(response, -1);
        stimuli = stimuli(:, :, keptFixation);
        response = response(keptFixation);
        assert(~ismember(-1, response));

        %% write stimuli, create data table
        stimuliPaths = writeStimuli(stimuli, stimuliFile, dryRun);
        [~, stimulusCategory] = fileparts(directory);
        stimulusCategory = repmat({stimulusCategory}, size(response));
        cellName = repmat({summary.cellid}, size(response));
        animal = repmat({summary.cellid(1)}, size(response));
        area = repmat({summary.area}, size(response));
        stimulusRepeats = repmat({summary.repcount}, size(response));
        data = table(stimuliPaths, response, stimulusCategory, cellName, area, animal, stimulusRepeats);
        
        %% write csv
        [responseFiledir, responseFilename, responseFileext] = fileparts(responseFile);
        dataFiledir = [responseFiledir, '/../data/'];
        csvPath = [dataFiledir, responseFilename, responseFileext, '.csv'];
        if ~dryRun
            if ~isfolder(dataFiledir)
                mkdir(dataFiledir);
            end
            writetable(data, csvPath);
        end
        fprintf('Wrote to %s\n', csvPath);
    end
end
end

function stimuliPaths = writeStimuli(stimuli, stimuliFile, dryRun)
    [stimuliFile, basename, basenameExt] = fileparts(stimuliFile); 
    [stimuliDir, baseDir, ~] = fileparts(stimuliFile);
    stimuliDir = [stimuliDir, '/stimuli/', baseDir, '/', basename, basenameExt];
    if isfolder(stimuliDir)
        dryRun = true;
    else
        mkdir(stimuliDir);
    end
    stimuliPaths = cell(size(stimuli, 3), 1);
    for stimulus = 1:size(stimuli, 3)
        image = stimuli(:, :, stimulus);
        stimuliPaths{stimulus} = [stimuliDir, '/', num2str(stimulus), '.jpg'];
        if ~dryRun
            assert(~isfile(stimuliPaths{stimulus}));
            imwrite(image, stimuliPaths{stimulus});
        end
    end
end
