/**
 * @file
 * @brief This file provides an interface for the ArgoDSM write buffer.
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#ifndef argo_write_buffer_hpp
#define argo_write_buffer_hpp argo_write_buffer_hpp

#include <deque>
#include <iterator>
#include <algorithm>

template<typename T>
class WriteBuffer
{
	private:
		/**
		 * @brief This container holds cache indexes that should be written back
		 */
		std::deque<T> _buffer;

	public:
		/**
		 * @brief True if buffer is empty, false otherwise.
		 */
		bool empty() {
			return _buffer.empty();
		}

		/**
		 * @brief Returns the size of the buffer.
		 */
		size_t size() {
			return _buffer.size();
		}

		/**
		 * @brief True if val is present in buffer, false otherwise.
		 */
		bool has(T val) {
			typename std::deque<T>::iterator it = std::find(_buffer.begin(),
					_buffer.end(), val);
			return (it != _buffer.end());
		}

		/**
		 * @brief Constructs a new element and emplaces it at the back of the buffer
		 */
		template<typename... Args>
			void emplace_back( Args&&... args) {
				_buffer.emplace_back(std::forward<Args>(args)...);
			}

		/**
		 * @brief Removes the front element from the buffer and returns it
		 */
		T pop() {
			auto elem = std::move(_buffer.front());
			_buffer.pop_front();
			return elem;
		}

		/**
		 * @brief If val exists in buffer, delete it. Else, do nothing.
		 */
		void erase(T val) {
			typename std::deque<T>::iterator it = std::find(_buffer.begin(),
					_buffer.end(), val);
			if(it != _buffer.end()){
				_buffer.erase(it);
			}
		}
}; //class

#endif /* argo_write_buffer_hpp */
