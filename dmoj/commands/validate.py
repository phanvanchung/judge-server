import os
import shlex
import subprocess
from itertools import groupby
from operator import itemgetter
from typing import List, Optional, Tuple, Union


from dmoj import executors
from dmoj.commands.base_command import Command
from dmoj.contrib import contrib_modules
from dmoj.error import CompileError, InvalidCommandException, OutputLimitExceeded
from dmoj.judgeenv import env, get_problem_root, get_supported_problems
from dmoj.problem import BatchedTestCase, Problem, ProblemConfig, ProblemDataManager, TestCase
from dmoj.result import Result
from dmoj.utils.ansi import print_ansi
from dmoj.utils.helper_files import compile_with_auxiliary_files
from dmoj.utils.unicode import utf8text


all_executors = executors.executors


class ValidateCommand(Command):
    name = 'validate'
    help = 'Validates input for problems.'

    def _populate_parser(self) -> None:
        self.arg_parser.add_argument('problem_ids', nargs='+', help='ids of problems to test')

    def execute(self, line: str) -> int:
        args = self.arg_parser.parse_args(line)

        problem_ids = args.problem_ids
        supported_problems = set(get_supported_problems())

        unknown_problems = ', '.join(f"'{i}'" for i in problem_ids if i not in supported_problems)
        if unknown_problems:
            raise InvalidCommandException(f'unknown problem(s) {unknown_problems}')
        total_fails = 0
        for problem_id in problem_ids:
            fails = self.validate_problem(problem_id)
            if fails:
                print_ansi(f'Problem #ansi[{problem_id}](cyan|bold) #ansi[failed](red|bold).')
                total_fails += 1
            else:
                print_ansi(f'Problem #ansi[{problem_id}](cyan|bold) passed with flying colours.')
            print()

        print()
        print('Test complete.')
        if total_fails:
            print_ansi(f'#ansi[A total of {total_fails} problem(s) have invalid input](red|bold)')
        else:
            print_ansi('#ansi[All problems validated.](green|bold)')

        return total_fails

    def validate_problem(self, problem_id: str) -> int:
        print_ansi(f'Validating problem #ansi[{problem_id}](cyan|bold)...')

        problem_root = get_problem_root(problem_id)
        config = ProblemConfig(ProblemDataManager(problem_root))

        if not config or 'validator' not in config:
            print_ansi('\t#ansi[Skipped](magenta|bold) - No validator found')
            return 0

        validator_config = config['validator']
        language = validator_config['language']
        if language not in all_executors:
            print_ansi('\t\t#ansi[Skipped](magenta|bold) - Language not supported')
            return 0
        time_limit = validator_config.get('time', env.generator_time_limit)
        memory_limit = validator_config.get('memory', env.generator_memory_limit)
        compiler_time_limit = validator_config.get('compiler_time_limit', env.generator_compiler_time_limit)
        read_feedback_from = validator_config.get('feedback', 'stderr')

        if read_feedback_from not in ('stdout', 'stderr'):
            print_ansi('\t\t#ansi[Skipped](magenta|bold) - Feedback option not supported')
            return 0

        if isinstance(validator_config.source, str):
            filenames = [validator_config.source]
        elif isinstance(validator_config.source.unwrap(), list):
            filenames = list(validator_config.source.unwrap())
        else:
            print_ansi('\t#ansi[Skipped](magenta|bold) - No validator found')
            return 0

        filenames = [os.path.abspath(os.path.join(problem_root, name)) for name in filenames]
        try:
            executor = compile_with_auxiliary_files(filenames, [], language, compiler_time_limit)
        except CompileError as compilation_error:
            error_msg = compilation_error.message or 'compiler exited abnormally'
            print_ansi('#ansi[Failed compiling validator!](red|bold)')
            print(error_msg.rstrip())
            return 1

        problem = Problem(problem_id, time_limit, memory_limit, {})

        grader_class = type('Grader', (problem.grader_class,), {'_generate_binary': lambda *_: None})  # don't compile
        grader = grader_class(self.judge, problem, language, b'')

        flattened_cases: List[Tuple[Optional[int], Union[TestCase, BatchedTestCase]]] = []
        batch_number = 0
        for case in grader.cases():
            if isinstance(case, BatchedTestCase):
                batch_number += 1
                for batched_case in case.batched_cases:
                    flattened_cases.append((batch_number, batched_case))
            else:
                flattened_cases.append((None, case))

        contrib_type = validator_config.get('type', 'default')
        if contrib_type not in contrib_modules:
            print_ansi(f'#ansi[{contrib_type} is not a valid contrib module!](red|bold)')
            return 1

        args_format_string = (
            validator_config.args_format_string
            or contrib_modules[contrib_type].ContribModule.get_validator_args_format_string()
        )

        case_number = 0
        fail = 0
        for batch_number, cases in groupby(flattened_cases, key=itemgetter(0)):
            if batch_number:
                print_ansi(f'#ansi[Batch #{batch_number}](yellow|bold)')
            for _, case in cases:
                case_number += 1

                result = Result(case)
                input = case.input_data()

                validator_args = shlex.split(args_format_string.format(batch_no=case.batch, case_no=case.position))
                process = executor.launch(
                    *validator_args,
                    time=time_limit,
                    memory=memory_limit,
                    symlinks=case.config.symlinks,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    wall_time=case.config.wall_time_factor * time_limit,
                )
                try:
                    proc_output, proc_error = process.communicate(
                        input, outlimit=case.config.output_limit_length, errlimit=1048576
                    )
                except OutputLimitExceeded:
                    proc_error = b''
                    process.kill()
                finally:
                    process.wait()

                executor.populate_result(proc_error, result, process)

                feedback = (
                    utf8text({'stdout': proc_output, 'stderr': proc_error}[read_feedback_from].rstrip())
                    or result.feedback
                )

                code = result.readable_codes()[0]
                colored_code = f'#ansi[{code}]({Result.COLORS_BYID[code]}|bold)'
                colored_feedback = f'(#ansi[{utf8text(feedback)}](|underline))' if feedback else ''
                case_padding = '  ' if batch_number is not None else ''
                print_ansi(f'{case_padding}Test case {case_number:2d} {colored_code:3s} {colored_feedback}')

                if result.result_flag:
                    fail = 1

        return fail
