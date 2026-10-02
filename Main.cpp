// ============================================================================
// Main.cpp
//
// SOL-AVES style JobSystem - Phase 1
//
// C++17
//
// 実装内容:
//
//  1. Job::Description
//  2. Job::Dispatch()
//  3. JobHandle
//  4. JobHandle::Wait()
//  5. Worker Thread Pool
//  6. Job Queue
//  7. Job完了通知
//  8. Exception伝播
//  9. Worker数自動決定
// 10. Job Overhead Benchmark
// 11. Serial vs Parallel Benchmark
// 12. Speedup計測
// 13. Worker Efficiency
// 14. Jobs/sec
// 15. Time/Job
// 16. Console簡易グラフ
//
// Phase 1:
//      Fiberはまだ使用しない。
//      Worker ThreadからのWait()は禁止。
//
// Phase 2以降:
//      Windows Fiberを導入する。
// ============================================================================

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>


// ============================================================================
// Job Namespace
// ============================================================================

namespace Job
{

    // ============================================================================
    // Forward Declaration
    // ============================================================================

    class JobHandle;


    // ============================================================================
    // Thread Local Worker Information
    //
    // 現在のThreadがJobSystem Workerかどうか。
    //
    // Phase 1ではWorker Thread上からWait()を呼ぶと
    // Workerそのものが停止するため禁止する。
    // ============================================================================

    thread_local bool gIsWorkerThread = false;

    thread_local uint32_t gWorkerIndex = UINT32_MAX;


    // ============================================================================
    // JobState
    //
    // JobHandleが保持するJobの実行状態。
    //
    // Dispatch
    //
    //      ↓
    //
    // JobState作成
    //
    //      ↓
    //
    // Worker実行
    //
    //      ↓
    //
    // Completed = true
    //
    //      ↓
    //
    // WaitしているThreadをnotify
    //
    // Phase 4ではここにWaiting Fiberを追加する予定。
    // ============================================================================

    struct JobState
    {
        std::atomic<bool> Completed{ false };

        std::mutex Mutex;

        std::condition_variable Condition;

        std::exception_ptr Exception;
    };


    // ============================================================================
    // Job Description
    //
    // 使用例:
    //
    // Job::Description desc("CharacterUpdate");
    //
    // desc.SetFunction([]()
    // {
    //     UpdateCharacter();
    // });
    //
    // JobHandle handle = Job::Dispatch(desc);
    //
    // handle.Wait();
    // ============================================================================

    struct Description
    {
        std::string Name;

        std::function<void()> Function;


        explicit Description(const char* name)
            : Name(name ? name : "UnnamedJob")
        {}


        template<class F>
        void SetFunction(F&& function)
        {
            Function =
                std::forward<F>(
                    function);
        }
    };


    // ============================================================================
    // JobHandle
    // ============================================================================

    class JobHandle
    {
    public:

        JobHandle() = default;


        // ------------------------------------------------------------------------
        // IsValid
        // ------------------------------------------------------------------------

        bool IsValid() const
        {
            return mState != nullptr;
        }


        // ------------------------------------------------------------------------
        // IsCompleted
        // ------------------------------------------------------------------------

        bool IsCompleted() const
        {
            if (!mState)
            {
                return true;
            }

            return
                mState->Completed.load(
                    std::memory_order_acquire);
        }


        // ------------------------------------------------------------------------
        // Wait
        //
        // Phase 1:
        //
        //      condition_variableを使用。
        //
        // Main Thread:
        //
        //      Wait
        //       ↓
        //      Thread Sleep
        //
        //
        // Phase 4:
        //
        //      Wait
        //       ↓
        //      Current FiberをWaitingへ
        //       ↓
        //      Scheduler FiberへSwitch
        //
        // へ変更予定。
        // ------------------------------------------------------------------------

        void Wait() const;


    private:

        friend class JobSystem;


        explicit JobHandle(
            std::shared_ptr<JobState> state)
            : mState(
                std::move(state))
        {}


    private:

        std::shared_ptr<JobState> mState;
    };


    // ============================================================================
    // JobItem
    //
    // JobQueue内部に格納される実データ。
    // ============================================================================

    struct JobItem
    {
        uint64_t Id = 0;

        std::string Name;

        std::function<void()> Function;

        std::shared_ptr<JobState> State;
    };


    // ============================================================================
    // JobSystem
    // ============================================================================

    class JobSystem
    {
    public:

        // ------------------------------------------------------------------------
        // Singleton
        //
        // Phase 1ではサンプルを1ファイルで完結させるため使用。
        //
        // SOL-AVES全体の設計としてはServiceContainerなどへ
        // 入れる方が適している。
        // ------------------------------------------------------------------------

        static JobSystem& Instance()
        {
            static JobSystem instance;

            return instance;
        }


        // ------------------------------------------------------------------------
        // Initialize
        //
        // workerCount == 0
        //
        // の場合、
        //
        // hardware_concurrency - 1
        //
        // を使用。
        //
        // Main Thread用に1Thread残す。
        // ------------------------------------------------------------------------

        void Initialize(
            uint32_t workerCount = 0)
        {
            std::lock_guard<std::mutex> lifecycleLock(
                mLifecycleMutex);


            if (mInitialized)
            {
                return;
            }


            if (workerCount == 0)
            {
                const uint32_t hardwareThreads =
                    std::thread::hardware_concurrency();


                if (hardwareThreads > 1)
                {
                    workerCount =
                        hardwareThreads - 1;
                }
                else
                {
                    workerCount = 1;
                }
            }


            mStopping = false;

            mWorkerCount = workerCount;


            mWorkers.reserve(
                workerCount);


            std::cout
                << "========================================\n";

            std::cout
                << " JobSystem Initialize\n";

            std::cout
                << "========================================\n";

            std::cout
                << "Hardware Threads : "
                << std::thread::hardware_concurrency()
                << "\n";

            std::cout
                << "Worker Threads   : "
                << workerCount
                << "\n";

            std::cout
                << "========================================\n\n";


            for (uint32_t i = 0;
                i < workerCount;
                ++i)
            {
                mWorkers.emplace_back(
                    [this, i]()
                    {
                        WorkerMain(i);
                    });
            }


            mInitialized = true;
        }


        // ------------------------------------------------------------------------
        // Shutdown
        //
        // QueueにJobが残っている場合は、
        // すべて実行してからWorkerを終了する。
        // ------------------------------------------------------------------------

        void Shutdown()
        {
            {
                std::lock_guard<std::mutex> lifecycleLock(
                    mLifecycleMutex);


                if (!mInitialized)
                {
                    return;
                }
            }


            {
                std::lock_guard<std::mutex> queueLock(
                    mQueueMutex);

                mStopping = true;
            }


            // 全Workerを起床させる。
            mQueueCondition.notify_all();


            for (std::thread& worker :
                mWorkers)
            {
                if (worker.joinable())
                {
                    worker.join();
                }
            }


            mWorkers.clear();


            {
                std::lock_guard<std::mutex> lifecycleLock(
                    mLifecycleMutex);

                mInitialized = false;
            }


            std::cout
                << "\n========================================\n";

            std::cout
                << " JobSystem Shutdown\n";

            std::cout
                << "========================================\n";
        }


        // ------------------------------------------------------------------------
        // Dispatch
        //
        // Description
        //
        //      ↓
        //
        // JobState作成
        //
        //      ↓
        //
        // JobQueue
        //
        //      ↓
        //
        // Worker起床
        //
        //      ↓
        //
        // JobHandle返却
        // ------------------------------------------------------------------------

        JobHandle Dispatch(
            const Description& description)
        {
            if (!description.Function)
            {
                throw std::runtime_error(
                    "Job function is empty.");
            }


            {
                std::lock_guard<std::mutex> lifecycleLock(
                    mLifecycleMutex);


                if (!mInitialized)
                {
                    throw std::runtime_error(
                        "JobSystem is not initialized.");
                }
            }


            auto state =
                std::make_shared<JobState>();


            JobItem job;

            job.Id =
                mNextJobId.fetch_add(
                    1,
                    std::memory_order_relaxed);

            job.Name =
                description.Name;

            job.Function =
                description.Function;

            job.State =
                state;


            {
                std::lock_guard<std::mutex> queueLock(
                    mQueueMutex);


                if (mStopping)
                {
                    throw std::runtime_error(
                        "JobSystem is stopping.");
                }


                mJobQueue.push(
                    std::move(job));
            }


            // Idle Workerを1つ起こす。
            mQueueCondition.notify_one();


            return
                JobHandle(
                    std::move(state));
        }


        // ------------------------------------------------------------------------
        // GetWorkerCount
        // ------------------------------------------------------------------------

        uint32_t GetWorkerCount() const
        {
            return mWorkerCount;
        }


        // ------------------------------------------------------------------------
        // GetPendingJobCount
        // ------------------------------------------------------------------------

        size_t GetPendingJobCount()
        {
            std::lock_guard<std::mutex> lock(
                mQueueMutex);

            return
                mJobQueue.size();
        }


    private:

        JobSystem() = default;


        ~JobSystem()
        {
            Shutdown();
        }


        JobSystem(
            const JobSystem&) = delete;


        JobSystem& operator=(
            const JobSystem&) = delete;


        // ========================================================================
        // WorkerMain
        //
        // 常駐Worker Thread
        //
        //
        //               Job Queue
        //
        //                  │
        //
        //          ┌───────┼───────┐
        //
        //          ▼       ▼       ▼
        //
        //       Worker0 Worker1 Worker2
        //
        //
        // WorkerはJobがなければsleep。
        //
        // Jobが入れば起床して処理。
        //
        // Phase 2ではこのWorker上にScheduler Fiberを載せる。
        // ========================================================================

        void WorkerMain(
            uint32_t workerIndex)
        {
            gIsWorkerThread = true;

            gWorkerIndex =
                workerIndex;


            while (true)
            {
                JobItem job;


                // =================================================================
                // Job取得
                // =================================================================

                {
                    std::unique_lock<std::mutex> lock(
                        mQueueMutex);


                    mQueueCondition.wait(
                        lock,
                        [this]()
                        {
                            return
                                mStopping ||
                                !mJobQueue.empty();
                        });


                    // Shutdown要求 && Jobなし
                    if (mStopping &&
                        mJobQueue.empty())
                    {
                        break;
                    }


                    job =
                        std::move(
                            mJobQueue.front());


                    mJobQueue.pop();
                }


                // =================================================================
                // Job実行
                // =================================================================

                try
                {
                    job.Function();
                }
                catch (...)
                {
                    std::lock_guard<std::mutex> stateLock(
                        job.State->Mutex);


                    job.State->Exception =
                        std::current_exception();
                }


                // =================================================================
                // Job完了
                // =================================================================

                {
                    std::lock_guard<std::mutex> stateLock(
                        job.State->Mutex);


                    job.State->Completed.store(
                        true,
                        std::memory_order_release);
                }


                // Wait中Threadを起こす。
                job.State->Condition.notify_all();
            }


            gWorkerIndex =
                UINT32_MAX;

            gIsWorkerThread =
                false;
        }


    private:

        // Lifecycle
        std::mutex mLifecycleMutex;

        bool mInitialized = false;


        // Queue
        std::mutex mQueueMutex;

        std::condition_variable mQueueCondition;

        std::queue<JobItem> mJobQueue;


        // Workers
        std::vector<std::thread> mWorkers;


        // Job ID
        std::atomic<uint64_t> mNextJobId{ 1 };


        // State
        bool mStopping = false;

        uint32_t mWorkerCount = 0;
    };


    // ============================================================================
    // JobHandle::Wait
    // ============================================================================

    void JobHandle::Wait() const
    {
        if (!mState)
        {
            return;
        }


        // ------------------------------------------------------------------------
        // 既に完了済み
        // ------------------------------------------------------------------------

        if (mState->Completed.load(
            std::memory_order_acquire))
        {
            if (mState->Exception)
            {
                std::rethrow_exception(
                    mState->Exception);
            }

            return;
        }


        // ------------------------------------------------------------------------
        // Phase 1ではWorker ThreadからWait禁止
        //
        //
        // Worker 0
        //
        //      Job A
        //
        //       ↓
        //
        //      Wait(Job B)
        //
        //
        // とするとWorkerそのものがSleepする。
        //
        //
        // 全WorkerがWaitすると、
        //
        // QueueにJob Bがあるのに実行者がいない
        //
        // というDeadlockになる。
        //
        // Phase 4でFiber Waitに置き換える。
        // ------------------------------------------------------------------------

        if (gIsWorkerThread)
        {
            throw std::runtime_error(
                "Phase 1: JobHandle::Wait() cannot be called "
                "from a Worker Thread. "
                "Fiber support will remove this restriction.");
        }


        std::unique_lock<std::mutex> lock(
            mState->Mutex);


        mState->Condition.wait(
            lock,
            [this]()
            {
                return
                    mState->Completed.load(
                        std::memory_order_acquire);
            });


        // Job内部でExceptionが発生していれば
        // Waitした側へ再throw。
        if (mState->Exception)
        {
            std::rethrow_exception(
                mState->Exception);
        }
    }


    // ============================================================================
    // Public API
    // ============================================================================

    void Initialize(
        uint32_t workerCount = 0)
    {
        JobSystem::Instance().Initialize(
            workerCount);
    }


    void Shutdown()
    {
        JobSystem::Instance().Shutdown();
    }


    JobHandle Dispatch(
        const Description& description)
    {
        return
            JobSystem::Instance().Dispatch(
                description);
    }


    uint32_t GetWorkerCount()
    {
        return
            JobSystem::Instance().GetWorkerCount();
    }


    size_t GetPendingJobCount()
    {
        return
            JobSystem::Instance().GetPendingJobCount();
    }

} // namespace Job


// ============================================================================
// Benchmark
// ============================================================================

using Clock =
std::chrono::steady_clock;


// ============================================================================
// ToMilliseconds
// ============================================================================

double ToMilliseconds(
    Clock::time_point begin,
    Clock::time_point end)
{
    return
        std::chrono::duration<
        double,
        std::milli>(
            end - begin)
        .count();
}


// ============================================================================
// BenchmarkWork
//
// CPU負荷用処理。
//
// 単純な加算だけだとコンパイラ最適化で削除される可能性があるため、
// XOR / Shift / Multiplyを使用する。
// ============================================================================

uint64_t BenchmarkWork(
    uint64_t seed,
    int iterations)
{
    uint64_t value =
        seed +
        0x9E3779B97F4A7C15ull;


    for (int i = 0;
        i < iterations;
        ++i)
    {
        value ^= value >> 12;

        value ^= value << 25;

        value ^= value >> 27;


        value *=
            0x2545F4914F6CDD1Dull;
    }


    return value;
}


// ============================================================================
// PrintTimeBar
//
// Console簡易グラフ
//
// Serial
//
// JobSystem
//
// の実行時間を視覚化。
// ============================================================================

void PrintTimeBar(
    const char* name,
    double milliseconds,
    double maxMilliseconds)
{
    constexpr int BarWidth = 40;


    int barLength = 0;


    if (maxMilliseconds > 0.0)
    {
        barLength =
            static_cast<int>(
                milliseconds /
                maxMilliseconds *
                static_cast<double>(
                    BarWidth));
    }


    barLength =
        std::clamp(
            barLength,
            1,
            BarWidth);


    std::cout
        << std::left
        << std::setw(12)
        << name
        << " |";


    for (int i = 0;
        i < barLength;
        ++i)
    {
        std::cout << '#';
    }


    for (int i = barLength;
        i < BarWidth;
        ++i)
    {
        std::cout << ' ';
    }


    std::cout
        << "| "
        << std::fixed
        << std::setprecision(3)
        << milliseconds
        << " ms\n";
}


// ============================================================================
// Main
// ============================================================================

int main()
{
    try
    {
        // ====================================================================
        // Initialize
        // ====================================================================

        Job::Initialize(0);


        const uint32_t workerCount =
            Job::GetWorkerCount();


        std::cout
            << "\n========================================\n";

        std::cout
            << " JobSystem Phase 1 Benchmark\n";

        std::cout
            << "========================================\n";

        std::cout
            << "Worker Count : "
            << workerCount
            << "\n";

        std::cout
            << "========================================\n\n";


        // ====================================================================
        //
        // TEST 1
        //
        // 基本動作
        //
        // 8 Jobを投げてThread IDを表示。
        //
        // ====================================================================

        {
            std::cout
                << "========================================\n";

            std::cout
                << " Test 1 : Basic Jobs\n";

            std::cout
                << "========================================\n";


            constexpr int TestJobCount = 8;


            std::mutex printMutex;


            std::vector<Job::JobHandle>
                handles;


            handles.reserve(
                TestJobCount);


            for (int i = 0;
                i < TestJobCount;
                ++i)
            {
                Job::Description desc(
                    "BasicJob");


                desc.SetFunction(
                    [i, &printMutex]()
                    {
                        uint64_t sum = 0;


                        for (uint64_t n = 0;
                            n < 1'000'000;
                            ++n)
                        {
                            sum += n;
                        }


                        std::lock_guard<std::mutex> lock(
                            printMutex);


                        std::cout
                            << "Job "
                            << i
                            << " finished"
                            << " | Thread = "
                            << std::this_thread::get_id()
                            << " | Sum = "
                            << sum
                            << "\n";
                    });


                handles.emplace_back(
                    Job::Dispatch(desc));
            }


            for (const Job::JobHandle& handle :
                handles)
            {
                handle.Wait();
            }


            std::cout
                << "\nBasic Jobs : SUCCESS\n\n";
        }


        // ====================================================================
        //
        // BENCHMARK 1
        //
        // JobSystem Overhead
        //
        //
        // Jobの中身をほぼ空にして、
        //
        // Dispatch
        //
        // Queue
        //
        // Wake
        //
        // Execute
        //
        // Complete
        //
        // Wait
        //
        // の管理コストを見る。
        //
        // ====================================================================

        {
            constexpr int JobCount =
                10000;


            std::cout
                << "========================================\n";

            std::cout
                << " Benchmark 1 : Job Overhead\n";

            std::cout
                << "========================================\n";

            std::cout
                << "Job Count : "
                << JobCount
                << "\n\n";


            std::atomic<uint64_t>
                counter{ 0 };


            std::vector<Job::JobHandle>
                handles;


            handles.reserve(
                JobCount);


            // ----------------------------------------------------------------
            // Total Start
            // ----------------------------------------------------------------

            const auto totalBegin =
                Clock::now();


            // ----------------------------------------------------------------
            // Dispatch Start
            // ----------------------------------------------------------------

            const auto dispatchBegin =
                Clock::now();


            for (int i = 0;
                i < JobCount;
                ++i)
            {
                Job::Description desc(
                    "OverheadJob");


                desc.SetFunction(
                    [&counter]()
                    {
                        counter.fetch_add(
                            1,
                            std::memory_order_relaxed);
                    });


                handles.emplace_back(
                    Job::Dispatch(desc));
            }


            const auto dispatchEnd =
                Clock::now();


            // ----------------------------------------------------------------
            // Wait
            // ----------------------------------------------------------------

            for (const Job::JobHandle& handle :
                handles)
            {
                handle.Wait();
            }


            const auto totalEnd =
                Clock::now();


            // ----------------------------------------------------------------
            // Result
            // ----------------------------------------------------------------

            const double dispatchMs =
                ToMilliseconds(
                    dispatchBegin,
                    dispatchEnd);


            const double totalMs =
                ToMilliseconds(
                    totalBegin,
                    totalEnd);


            const double jobsPerSecond =
                static_cast<double>(
                    JobCount) /
                (totalMs / 1000.0);


            const double microsecondsPerJob =
                (
                    totalMs *
                    1000.0
                    ) /
                static_cast<double>(
                    JobCount);


            const double dispatchMicrosecondsPerJob =
                (
                    dispatchMs *
                    1000.0
                    ) /
                static_cast<double>(
                    JobCount);


            std::cout
                << std::fixed
                << std::setprecision(3);


            std::cout
                << "Dispatch Time       : "
                << dispatchMs
                << " ms\n";


            std::cout
                << "Total Time          : "
                << totalMs
                << " ms\n";


            std::cout
                << "Dispatch / Job      : "
                << dispatchMicrosecondsPerJob
                << " us\n";


            std::cout
                << "Total Time / Job    : "
                << microsecondsPerJob
                << " us\n";


            std::cout
                << std::setprecision(0);


            std::cout
                << "Jobs / sec          : "
                << jobsPerSecond
                << "\n";


            std::cout
                << "Completed           : "
                << counter.load()
                << " / "
                << JobCount
                << "\n";


            if (counter.load() ==
                JobCount)
            {
                std::cout
                    << "Result              : SUCCESS\n";
            }
            else
            {
                std::cout
                    << "Result              : FAILED\n";
            }


            std::cout
                << "\n";
        }


        // ====================================================================
        //
        // BENCHMARK 2
        //
        // Serial VS JobSystem
        //
        //
        // 同じCPU処理を
        //
        //
        // Main Threadのみ
        //
        //        VS
        //
        // JobSystem
        //
        //
        // で比較。
        //
        // ====================================================================

        {
            constexpr int JobCount =
                2000;


            constexpr int WorkIterations =
                100000;


            std::cout
                << "========================================\n";

            std::cout
                << " Benchmark 2 : Serial vs JobSystem\n";

            std::cout
                << "========================================\n";


            std::cout
                << "Job Count       : "
                << JobCount
                << "\n";


            std::cout
                << "Iterations/Job  : "
                << WorkIterations
                << "\n";


            std::cout
                << "Worker Count    : "
                << workerCount
                << "\n\n";


            // =================================================================
            // SERIAL
            // =================================================================

            uint64_t serialChecksum = 0;


            const auto serialBegin =
                Clock::now();


            for (int i = 0;
                i < JobCount;
                ++i)
            {
                const uint64_t result =
                    BenchmarkWork(
                        static_cast<uint64_t>(
                            i),
                        WorkIterations);


                serialChecksum ^=
                    result;
            }


            const auto serialEnd =
                Clock::now();


            const double serialMs =
                ToMilliseconds(
                    serialBegin,
                    serialEnd);


            // =================================================================
            // PARALLEL
            // =================================================================

            std::atomic<uint64_t>
                parallelChecksum{ 0 };


            std::vector<Job::JobHandle>
                handles;


            handles.reserve(
                JobCount);


            const auto parallelBegin =
                Clock::now();


            const auto dispatchBegin =
                Clock::now();


            for (int i = 0;
                i < JobCount;
                ++i)
            {
                Job::Description desc(
                    "CpuBenchmarkJob");


                desc.SetFunction(
                    [
                        i,
                        &parallelChecksum
                    ]()
                    {
                        const uint64_t result =
                            BenchmarkWork(
                                static_cast<uint64_t>(
                                    i),
                                WorkIterations);


                        parallelChecksum.fetch_xor(
                            result,
                            std::memory_order_relaxed);
                    });


                handles.emplace_back(
                    Job::Dispatch(desc));
            }


            const auto dispatchEnd =
                Clock::now();


            // ----------------------------------------------------------------
            // Wait
            // ----------------------------------------------------------------

            for (const Job::JobHandle& handle :
                handles)
            {
                handle.Wait();
            }


            const auto parallelEnd =
                Clock::now();


            // =================================================================
            // Result
            // =================================================================

            const double parallelMs =
                ToMilliseconds(
                    parallelBegin,
                    parallelEnd);


            const double dispatchMs =
                ToMilliseconds(
                    dispatchBegin,
                    dispatchEnd);


            const double speedup =
                serialMs /
                parallelMs;


            const double efficiency =
                (
                    speedup /
                    static_cast<double>(
                        workerCount)
                    ) *
                100.0;


            const double parallelJobsPerSecond =
                static_cast<double>(
                    JobCount) /
                (parallelMs / 1000.0);


            const double serialJobsPerSecond =
                static_cast<double>(
                    JobCount) /
                (serialMs / 1000.0);


            // ----------------------------------------------------------------
            // Console
            // ----------------------------------------------------------------

            std::cout
                << std::fixed
                << std::setprecision(3);


            std::cout
                << "Serial Time        : "
                << serialMs
                << " ms\n";


            std::cout
                << "JobSystem Time     : "
                << parallelMs
                << " ms\n";


            std::cout
                << "Dispatch Time      : "
                << dispatchMs
                << " ms\n";


            std::cout
                << "Speedup            : "
                << speedup
                << " x\n";


            std::cout
                << "Worker Efficiency  : "
                << efficiency
                << " %\n";


            std::cout
                << std::setprecision(0);


            std::cout
                << "Serial Jobs/sec    : "
                << serialJobsPerSecond
                << "\n";


            std::cout
                << "Parallel Jobs/sec  : "
                << parallelJobsPerSecond
                << "\n";


            // =================================================================
            // Checksum
            // =================================================================

            const uint64_t parallelChecksumValue =
                parallelChecksum.load(
                    std::memory_order_relaxed);


            std::cout
                << std::hex;


            std::cout
                << "Serial Checksum    : 0x"
                << serialChecksum
                << "\n";


            std::cout
                << "Parallel Checksum  : 0x"
                << parallelChecksumValue
                << "\n";


            std::cout
                << std::dec;


            // =================================================================
            // Graph
            // =================================================================

            std::cout
                << "\n----------------------------------------\n";

            std::cout
                << " Execution Time Graph\n";

            std::cout
                << " Lower is better\n";

            std::cout
                << "----------------------------------------\n";


            const double maxTime =
                std::max(
                    serialMs,
                    parallelMs);


            PrintTimeBar(
                "Serial",
                serialMs,
                maxTime);


            PrintTimeBar(
                "JobSystem",
                parallelMs,
                maxTime);


            std::cout
                << "\n----------------------------------------\n";


            std::cout
                << std::fixed
                << std::setprecision(2);


            std::cout
                << " Speedup           : "
                << speedup
                << " x\n";


            std::cout
                << " Worker Efficiency : "
                << efficiency
                << " %\n";


            std::cout
                << "----------------------------------------\n";


            // =================================================================
            // Correctness
            // =================================================================

            if (serialChecksum ==
                parallelChecksumValue)
            {
                std::cout
                    << "\nChecksum : OK\n";
            }
            else
            {
                std::cout
                    << "\nChecksum : ERROR\n";
            }


            // =================================================================
            // Evaluation
            // =================================================================

            std::cout
                << "\n========================================\n";

            std::cout
                << " Performance Evaluation\n";

            std::cout
                << "========================================\n";


            if (speedup > 1.0)
            {
                std::cout
                    << "Parallel execution is faster.\n";
            }
            else
            {
                std::cout
                    << "Parallel execution is not faster.\n";
            }


            if (speedup >=
                static_cast<double>(
                    workerCount) *
                0.75)
            {
                std::cout
                    << "Scaling : Excellent\n";
            }
            else if (speedup >=
                static_cast<double>(
                    workerCount) *
                0.50)
            {
                std::cout
                    << "Scaling : Good\n";
            }
            else if (speedup > 1.0)
            {
                std::cout
                    << "Scaling : Limited\n";
            }
            else
            {
                std::cout
                    << "Scaling : No benefit\n";
            }


            std::cout
                << "========================================\n";
        }


        // ====================================================================
        // Shutdown
        // ====================================================================

        Job::Shutdown();
    }
    catch (const std::exception& e)
    {
        std::cerr
            << "\n========================================\n";

        std::cerr
            << " ERROR\n";

        std::cerr
            << "========================================\n";

        std::cerr
            << e.what()
            << "\n";


        Job::Shutdown();


        return 1;
    }


    return 0;
}