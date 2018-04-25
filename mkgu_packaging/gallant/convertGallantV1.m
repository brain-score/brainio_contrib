function convertGallantV1(dryRun)
if ~exist('dryRun', 'var')
    dryRun = false;
end
addpath(genpath('gallant/V1Data/functions'));
directory = 'gallant/V1Data/NatRev';
files = glob([directory, '/*/*.mat']);
for i = 1:length(files)
    file = files{i};
    [subDir, ~, ~] = fileparts(file);
    summaries = load(file);
    summaries = summaries.celldata;
    for summary = summaries'
        stimuliFile = [subDir, '/', summary.rawstimfile];
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

        stimuliPaths = writeStimuli(stimuli, stimuliFile, dryRun);
        if ~stimuliPaths
            fprintf('Skipping\n');
            continue;
        end
        cellName = repmat({summary.cellid}, size(response));
        data = table(stimuliPaths, response, cellName);
        [responseFiledir, responseFilename, responseFileext] = fileparts(responseFile);
        csvPath = [responseFiledir, '/../data/', responseFilename, responseFileext, '.csv'];
        if ~dryRun
            writetable(data, csvPath);
        end
        fprintf('Wrote to %s\n', csvPath);
        %implay(uint8(stimuli));
    end
end
end

function stimuliPaths = writeStimuli(stimuli, stimuliFile, dryRun)
    [stimuliFile, basename, basenameExt] = fileparts(stimuliFile); 
    [stimuliDir, baseDir, ~] = fileparts(stimuliFile);
    stimuliDir = [stimuliDir, '/stimuli/', baseDir, '/', basename, basenameExt];
    if isfolder(stimuliDir)
        return false;  % indicate already complete
    end
    mkdir(stimuliDir);
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
