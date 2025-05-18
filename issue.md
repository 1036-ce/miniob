While reading the code under `src/observer/storage/buffer`, I came across the definitions of `BPFileHeader` and `Page`. Here are the relevant excerpts:

```cpp
struct BPFileHeader
{
  int32_t buffer_pool_id;   //! buffer pool id
  int32_t page_count;       //! 当前文件一共有多少个页面
  int32_t allocated_pages;  //! 已经分配了多少个页面
  char    bitmap[0];        //! 页面分配位图, 第0个页面(就是当前页面)，总是1

  /**
   * 能够分配的最大的页面个数，即bitmap的字节数 乘以8
   */
  static const int MAX_PAGE_NUM = (BP_PAGE_DATA_SIZE - sizeof(page_count) - sizeof(allocated_pages)) * 8;

  string to_string() const;
};

```

```cpp
static constexpr const int BP_PAGE_SIZE      = (1 << 13);
static constexpr const int BP_PAGE_DATA_SIZE = (BP_PAGE_SIZE - sizeof(PageNum) - sizeof(LSN) - sizeof(CheckSum));

/**
 * @brief 表示一个页面，可能放在内存或磁盘上
 * @ingroup BufferPool
 */
struct Page
{
  LSN      lsn;
  CheckSum check_sum;
  char     data[BP_PAGE_DATA_SIZE];
};
```

I have a couple of questions regarding the logic of these definitions:

For `BPFileHeader::MAX_PAGE_NUM`:
The current calculation is:

```cpp
MAX_PAGE_NUM = (BP_PAGE_DATA_SIZE - sizeof(page_count) - sizeof(allocated_pages)) * 8;
```
However, the BPFileHeader struct also includes a buffer_pool_id field, which does not appear to be accounted for in the calculation. Shouldn’t it be:

```cpp
MAX_PAGE_NUM = (BP_PAGE_DATA_SIZE - sizeof(buffer_pool_id) - sizeof(page_count) - sizeof(allocated_pages)) * 8;
```
Or is there a reason buffer_pool_id is intentionally excluded?

For BP_PAGE_DATA_SIZE:
It is currently defined as:

```cpp
BP_PAGE_DATA_SIZE = BP_PAGE_SIZE - sizeof(PageNum) - sizeof(LSN) - sizeof(CheckSum);
```

But PageNum does not appear in the Page struct itself. Is this a legacy field or something expected to be handled externally?

I tried modifying both definitions as above:

```cpp
/* src/observer/storage/buffer/disk_buffer_pool.h */
// static const int MAX_PAGE_NUM = (BP_PAGE_DATA_SIZE - sizeof(page_count) - sizeof(allocated_pages)) * 8;
static const int MAX_PAGE_NUM = (BP_PAGE_DATA_SIZE - sizeof(buffer_pool_id) - sizeof(page_count) - sizeof(allocated_pages)) * 8;

/* src/observer/storage/buffer/page.h */
// static constexpr const int BP_PAGE_DATA_SIZE = (BP_PAGE_SIZE - sizeof(PageNum) - sizeof(LSN) - sizeof(CheckSum));
static constexpr const int BP_PAGE_DATA_SIZE = (BP_PAGE_SIZE - sizeof(LSN) - sizeof(CheckSum));
```

After making these changes, the code still passes the basic test successfully.

I’m not sure if I’m misunderstanding something, or if there are design details I’ve overlooked. Any clarification would be greatly appreciated!

Thanks in advance.

